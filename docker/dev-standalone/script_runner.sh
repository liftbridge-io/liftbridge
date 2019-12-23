#!/bin/ash

# turn on ash's job control
set -m

# Start the nats-server and put it in the background
nats-server -c nats-server.conf &

while ! nc -z localhost 8222; do sleep 1 ; echo 'waiting nats server' ; done

# Start liftbridge
liftbridge --raft-bootstrap-seed

fg %1
