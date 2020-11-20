FROM golang:1.14-alpine as build-base

ENV GO111MODULE on
RUN go get github.com/liftbridge-io/liftbridge@master
RUN go get github.com/nats-io/nats-server/v2

FROM alpine:latest
COPY --from=build-base /go/bin/liftbridge /usr/local/bin/liftbridge
COPY --from=build-base /go/bin/nats-server /usr/local/bin/nats-server

EXPOSE 9292 4222 8222 6222

VOLUME "/tmp/liftbridge/liftbridge-default"

COPY docker/dev-standalone/nats-server.conf nats-server.conf
COPY docker/dev-standalone/liftbridge.yaml liftbridge.yaml
COPY docker/dev-standalone/script_runner.sh script_runner.sh

RUN chmod +x script_runner.sh
CMD ./script_runner.sh
