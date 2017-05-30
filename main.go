package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
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

type posInfo struct {
	logFile string
	inode   uint64
	offset  int64
}

func sendToSignalFX(timestamp time.Time) error {
	points := []*datapoint.Datapoint{}
	dimensions := map[string]string{
		"hostname": config.HOSTNAME,
		"scope":    config.ENV_SCOPE,
	}

	datum := sfxclient.Gauge("fluentd-kinesis-forwarder-monitor", dimensions, timestamp.Unix())
	points = append(points, datum)

	return sfxSink.AddDatapoints(context.Background(), points)
}

func main() {
	config.Initialize()

	sfxSink.AuthToken = config.SIGNALFX_API_KEY

	for {
		ts, context, err := trackTimestamp(config.LOG_FILE_POS)
		if err != nil {
			log.ErrorD("track-timestamp", logger.M{"msg": err.Error()})
		} else {
			log.GaugeIntD("track-timestamp", int(ts.UnixNano()), logger.M{
				"latest-log-ts": ts.String(), "context": context, "val-units": "nsec",
			})

			err = sendToSignalFX(ts)
			if err != nil {
				log.ErrorD("send-to-signalfx", logger.M{"msg": err.Error()})
			}
		}

		time.Sleep(10 * time.Second)
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

	// pos.offset is the position of the \n of the late read log line.  +1 to skip \n.
	line, err := readLine(pos.logFile, pos.offset+1)
	if err == io.EOF { // If byte is at the end of file, fluentd is caught up
		return time.Now(), "byte offset points to eof", nil
	}
	if err != nil {
		return time.Time{}, "", err
	}

	if len(line) < 32 {
		return time.Time{}, "", fmt.Errorf("No timestamp found")
	}

	ts, err := time.Parse(time.RFC3339Nano, "2017-05-21T22:49:23.314299+00:00")
	if err != nil {
		return time.Time{}, "", err
	}

	return ts, "parsed from log line", nil
}
