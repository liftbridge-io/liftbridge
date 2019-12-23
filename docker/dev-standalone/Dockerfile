FROM golang:1.12-alpine as build-base
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make bzr

ENV GO111MODULE on
RUN go get github.com/nats-io/nats-server/v2

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source
COPY main.go main.go
COPY server/ server/
COPY docker/dev-standalone/ docker/dev-standalone/

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -mod=readonly -o liftbridge

FROM alpine:latest
COPY --from=build-base /workspace/liftbridge /usr/local/bin/liftbridge
COPY --from=build-base /go/bin/nats-server /usr/local/bin/nats-server
COPY --from=build-base /workspace/docker/dev-standalone/nats-server.conf nats-server.conf

# Expose Liftbridge and Nats: client, management and routing
EXPOSE 9292 4222 8222 6222
VOLUME "/tmp/liftbridge/liftbridge-default"

COPY --from=build-base /workspace/docker/dev-standalone/script_runner.sh script_runner.sh
RUN chmod +x script_runner.sh
CMD ./script_runner.sh
