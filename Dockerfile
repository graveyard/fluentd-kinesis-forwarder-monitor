FROM gliderlabs/alpine:3.3

ENTRYPOINT ["/bin/fluentd-kinesis-forwarder-monitor"]

RUN apk-install ca-certificates

COPY fluentd-kinesis-forwarder-monitor /bin/fluentd-kinesis-forwarder-monitor
