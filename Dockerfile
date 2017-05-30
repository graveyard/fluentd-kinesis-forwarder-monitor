FROM gliderlabs/alpine:3.3

RUN apk-install ca-certificates

COPY fluentd-kinesis-forwarder-monitor /bin/fluentd-kinesis-forwarder-monitor

ENTRYPOINT ["/bin/fluentd-kinesis-forwarder-monitor"]
