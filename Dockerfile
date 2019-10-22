FROM golang:1.13-alpine as build-base
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make bzr
ADD . /go/src/github.com/liftbridge-io/liftbridge
WORKDIR /go/src/github.com/liftbridge-io/liftbridge
ENV GO111MODULE on
ENV CGO_ENABLED 0
ENV GOARCH amd64
ENV GOOS linux
RUN go build -mod=readonly -o liftbridge

FROM alpine:latest
COPY --from=build-base /go/src/github.com/liftbridge-io/liftbridge/liftbridge /usr/local/bin/liftbridge
EXPOSE 9292
VOLUME "/tmp/liftbridge/liftbridge-default"
ENTRYPOINT ["liftbridge"]
