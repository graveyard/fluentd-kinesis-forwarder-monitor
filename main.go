package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	l "log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/signalfx/golib/datapoint"
	"github.com/signalfx/golib/sfxclient"

	"gopkg.in/Clever/kayvee-go.v6/logger"
)

var log = logger.New("fluentd-kinesis-forwarder-monitor")
var sfxSink = sfxclient.NewHTTPSink()
var hostname, scope, logsFile, posFile string

func getEnv(envVar string) string {
	val := os.Getenv(envVar)
	if val == "" {
		l.Fatalf("Must specify env variable %s", envVar)
	}
	return val
}
func init() {
	scope = getEnv("ENV_SCOPE")
	logsFile = getEnv("LOG_FILE")
	posFile = getEnv("LOG_FILE_POS")
	sfxSink.AuthToken = getEnv("SIGNALFX_API_KEY")

	host, err := os.Hostname()
	if err != nil {
		l.Fatal(err)
	}
	hostname = host
}

func sendToSignalFX(timestamp time.Time) error {
	points := []*datapoint.Datapoint{}
	dimensions := map[string]string{
		"hostname": hostname,
		"scope":    scope,
	}

	datum := sfxclient.Gauge("fluentd-kinesis-forwarder-monitor", dimensions, timestamp.Unix())
	points = append(points, datum)

	return sfxSink.AddDatapoints(context.Background(), points)
}

func main() {
	for {
		ts, err := trackTimeStamp("./messages", "./messages.fluentd.pos")
		if err != nil {
			log.ErrorD("track-timestamp", logger.M{"msg": err.Error()})
		} else {
			log.InfoD("track-timestamp", logger.M{"timestamp": ts.String()})

			err = sendToSignalFX(ts)
			if err != nil {
				log.ErrorD("send-to-signalfx", logger.M{"msg": err.Error()})
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func readLine(input io.ReadSeeker, start int64) (string, error) {
	if _, err := input.Seek(start, 0); err != nil {
		return "", err
	}

	r := bufio.NewReader(input)
	data, err := r.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return "", err
	}

	if len(data) > 0 && data[len(data)-1] == '\n' { // Trim off '\n'
		data = data[:len(data)-1]
	}
	return string(data), err
}

func trackTimeStamp(logsFile, posFile string) (time.Time, error) {
	data, err := ioutil.ReadFile(posFile)
	if err != nil {
		return time.Time{}, err
	}
	parts := strings.Split(string(data), "\t")
	if len(parts) != 3 {
		return time.Time{}, fmt.Errorf("error reading pos file: '%s'", string(data))
	}
	offset, err := strconv.ParseInt(parts[1], 16, 64)
	if err != nil {
		return time.Time{}, err
	}

	file, err := os.Open(logsFile)
	if err != nil {
		return time.Time{}, err
	}

	line, err := readLine(file, offset+1)
	if err != nil {
		return time.Time{}, err
	}

	if len(line) < 32 {
		return time.Time{}, fmt.Errorf("No timestamp found")
	}

	return time.Parse(time.RFC3339Nano, "2017-05-21T22:49:23.314299+00:00")
}
