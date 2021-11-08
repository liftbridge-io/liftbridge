FROM golang:1.17-alpine as build-base

ENV GO111MODULE on
RUN go get github.com/liftbridge-io/liftbridge@master

FROM alpine:latest
COPY --from=build-base /go/bin/liftbridge /usr/local/bin/liftbridge

EXPOSE 9292 4222 8222 6222

VOLUME "/tmp/liftbridge/liftbridge-default"

COPY docker/dev-standalone/nats-server.conf nats-server.conf
COPY docker/dev-standalone/liftbridge.yaml liftbridge.yaml

ENTRYPOINT ["liftbridge", "-c", "liftbridge.yaml"]
