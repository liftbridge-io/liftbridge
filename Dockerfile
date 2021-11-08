FROM golang:1.17-alpine as build-base
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh make
ADD . /go/src/github.com/liftbridge-io/liftbridge
WORKDIR /go/src/github.com/liftbridge-io/liftbridge
ENV GO111MODULE on
ENV CGO_ENABLED 0
ENV GOARCH amd64
ENV GOOS linux
RUN go build -mod=readonly -o liftbridge

FROM alpine:latest
RUN addgroup -g 1001 -S liftbridge && adduser -u 1001 -S liftbridge -G liftbridge
COPY --chown=liftbridge:liftbridge --from=build-base /go/src/github.com/liftbridge-io/liftbridge/liftbridge /usr/local/bin/liftbridge
EXPOSE 9292
VOLUME "/tmp/liftbridge/liftbridge-default"
ENTRYPOINT ["liftbridge"]
USER liftbridge
