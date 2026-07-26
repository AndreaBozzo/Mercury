FROM nats:2.14.3-alpine3.22@sha256:c11af972c99ae542de8925e6a7d9c533aa1eb039660420d2074beed6089b3bf0

RUN addgroup -S -g 10001 mercury \
    && adduser -S -D -H -u 10001 -G mercury mercury \
    && mkdir -p /data \
    && chown -R mercury:mercury /data

USER mercury
