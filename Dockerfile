FROM golang:1.9-alpine as build-base
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make
ADD . /go/src/github.com/liftbridge-io/liftbridge
WORKDIR /go/src/github.com/liftbridge-io/liftbridge
RUN go get golang.org/x/net/...
RUN GOOS=linux GOARCH=amd64 go build

FROM alpine:latest
COPY --from=build-base /go/src/github.com/liftbridge-io/liftbridge/liftbridge /usr/local/bin/liftbridge
EXPOSE 9292
VOLUME "/tmp/liftbridge/liftbridge-default"
ENTRYPOINT ["liftbridge"]
