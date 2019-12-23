FROM golang:1.12-alpine as build-base
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make bzr

WORKDIR /workspace

ENV GO111MODULE on
RUN go get github.com/nats-io/nats-server/v2

COPY docker/dev-standalone/ docker/dev-standalone/
COPY liftbridge-dev liftbridge-dev

FROM alpine:latest
COPY --from=build-base /workspace/liftbridge-dev /usr/local/bin/liftbridge
COPY --from=build-base /go/bin/nats-server /usr/local/bin/nats-server
COPY --from=build-base /workspace/docker/dev-standalone/nats-server.conf nats-server.conf

# Expose Liftbridge and Nats: client, management and routing
EXPOSE 9292 4222 8222 6222
VOLUME "/tmp/liftbridge/liftbridge-default"

COPY --from=build-base /workspace/docker/dev-standalone/script_runner.sh script_runner.sh

RUN chmod +x script_runner.sh
CMD ./script_runner.sh
