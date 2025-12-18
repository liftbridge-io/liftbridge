FROM golang:1.25-alpine as build-base

ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
RUN go build -mod=readonly \
    -ldflags "-s -w -X github.com/liftbridge-io/liftbridge/server.Version=${VERSION}" \
    -o liftbridge

FROM alpine:latest
RUN addgroup -g 1001 -S liftbridge && adduser -u 1001 -S liftbridge -G liftbridge
COPY --chown=liftbridge:liftbridge --from=build-base /workspace/liftbridge /usr/local/bin/liftbridge
EXPOSE 9292
VOLUME "/tmp/liftbridge/liftbridge-default"
ENTRYPOINT ["liftbridge"]
USER liftbridge
