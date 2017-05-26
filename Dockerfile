FROM gliderlabs/alpine:3.3

ENTRYPOINT ["/bin/fluentd-kinesis-forwarder-monitor"]

RUN apk-install ca-certificates

COPY catapult /bin/fluentd-kinesis-forwarder-monitor
