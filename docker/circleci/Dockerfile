FROM circleci/golang:1.17

RUN sudo apt-get update -yqq && sudo apt-get install -yqq bzr

WORKDIR /project

ENV GO111MODULE="on"

COPY go.mod /project/go.mod
COPY go.sum /project/go.sum
# cache deps before building and copying source so that we don't need to
# re-download as much and so that source changes don't invalidate our downloaded
# layer
RUN go mod download
