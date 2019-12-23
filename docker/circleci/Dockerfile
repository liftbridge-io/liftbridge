FROM circleci/golang:1.12

RUN sudo apt-get update -yqq && sudo apt-get install -yqq bzr

ENV GO111MODULE="on"

RUN go get github.com/mattn/goveralls
