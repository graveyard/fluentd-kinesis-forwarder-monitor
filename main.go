package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"

	"gopkg.in/Clever/kayvee-go.v6/logger"

	"github.com/Clever/fluentd-kinesis-forwarder-monitor/config"
)

var log = logger.New("fluentd-kinesis-forwarder-monitor")
var sfxSink = sfxclient.NewHTTPSink()
var hostname string

type posInfo struct {
	logFile string
	inode   uint64
	offset  int64
}

func sendToSignalFX(delay int64) error {
	points := []*datapoint.Datapoint{}
	dimensions := map[string]string{
		"hostname": hostname,
		"scope":    config.ENV_SCOPE,
	}

	datum := sfxclient.Gauge("fluentd-kinesis-forwarder-monitor.delay", dimensions, delay)
	points = append(points, datum)

	return sfxSink.AddDatapoints(context.Background(), points)
}

func getHostname() string {
	transport := &http.Transport{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, time.Duration(3*time.Second))
		},
	}

	client := &http.Client{Transport: transport}

	res, err := client.Get("http://169.254.169.254/latest/meta-data/local-ipv4")
	if err != nil {
		log.ErrorD("meta-data-request-failed", logger.M{"msg": err.Error()})
		return "unknown ip"
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.ErrorD("meta-data-parse-failed", logger.M{"msg": err.Error()})
		return "unknown ip"
	}

	hostname = "ip-" + strings.Replace(string(body), ".", "-", -1)

	return hostname
}

func heartbeat() {
	for _ = range time.Tick(15 * time.Second) {
		log.Info("heartbeat")
	}
}

func main() {
	config.Initialize()

	go heartbeat()

	sfxSink.AuthToken = config.SIGNALFX_API_KEY

	hostname = getHostname()

	count := 0
	for {
		ts, context, err := trackTimestamp(config.LOG_FILE_POS)
		if err != nil {
			log.ErrorD("track-timestamp", logger.M{"msg": err.Error()})
		} else {
			delay := time.Now().Unix() - ts.Unix()

			if count == 0 {
				log.GaugeIntD("track-timestamp", int(delay), logger.M{
					"latest-log-ts": ts.String(), "context": context, "val-units": "sec",
				})
			}

			err = sendToSignalFX(delay)
			if err != nil {
				log.ErrorD("send-to-signalfx", logger.M{"msg": err.Error()})
			}
		}

		count = (count + 1) % 4
		time.Sleep(15 * time.Second)
	}
}

func readLine(path string, start int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}

	if _, err := file.Seek(start, 0); err != nil {
		return "", err
	}

	r := bufio.NewReader(file)
	data, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}

	if len(data) > 0 && data[len(data)-1] == '\n' { // Trim off '\n'
		data = data[:len(data)-1]
	}
	return string(data), nil
}

// parsePositionData parses fluentd position files which have the following format:
// <file-path> \t <byte offset> \t <inode> \n
func parsePositionData(data []byte) (posInfo, error) {
	parts := strings.Split(string(data), "\t")
	if len(parts) != 3 {
		return posInfo{}, fmt.Errorf("error parsing pos file: '%s'", string(data))
	}

	logFile := parts[0]
	if logFile == "" {
		return posInfo{}, fmt.Errorf("Log file path not found in position file")
	}

	offset, err := strconv.ParseInt(parts[1], 16, 64)
	if err != nil {
		return posInfo{}, err
	}

	fileINode, err := strconv.ParseUint(strings.Trim(parts[2], "\n"), 16, 64)
	if err != nil {
		return posInfo{}, err
	}

	return posInfo{
		logFile: logFile,
		inode:   fileINode,
		offset:  offset,
	}, nil
}

func trackTimestamp(posFile string) (time.Time, string, error) {
	data, err := ioutil.ReadFile(posFile)
	if err != nil {
		return time.Time{}, "", err
	}
	pos, err := parsePositionData(data)
	if err != nil {
		return time.Time{}, "", err
	}

	fileinfo, err := os.Stat(pos.logFile)
	if err != nil {
		return time.Time{}, "", err
	}
	stat, ok := fileinfo.Sys().(*syscall.Stat_t)
	if !ok {
		return time.Time{}, "", fmt.Errorf("Failed to retrieve log file's inode")
	}

	if stat.Ino != pos.inode { // Differnt inodes means a log rotation has occured
		// Return creation time of the current log file.  This overestimates how far along fluentd
		// is, but that should okay for our purposes
		return time.Unix(int64(stat.Ctim.Sec), int64(stat.Ctim.Nsec)), "file rotate detected", nil
	}

	line, err := readLine(pos.logFile, pos.offset)
	if err == io.EOF { // If byte is at the end of file, fluentd is caught up
		return time.Now(), "byte offset points to eof", nil
	}
	if err != nil {
		return time.Time{}, "", err
	}

	if len(line) < 32 {
		return time.Time{}, "", fmt.Errorf("No timestamp found")
	}

	ts, err := time.Parse(time.RFC3339Nano, line[:32])
	if err != nil {
		ts, err = time.Parse(time.Stamp, line[:25]) // timestamps in Rsyslog_TraditionalFileFormat

		if err != nil {
			return time.Time{}, "", err
		}
	}

	return ts, "parsed from log line", nil
}
