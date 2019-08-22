FROM golang:1.12-alpine as build-base
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make bzr
ADD . /go/src/github.com/liftbridge-io/liftbridge
WORKDIR /go/src/github.com/liftbridge-io/liftbridge
ENV GO111MODULE on
RUN GOOS=linux GOARCH=amd64 go build

FROM alpine:latest
COPY --from=build-base /go/src/github.com/liftbridge-io/liftbridge/liftbridge /usr/local/bin/liftbridge
EXPOSE 9292
VOLUME "/tmp/liftbridge/liftbridge-default"
ENTRYPOINT ["liftbridge"]
