#!/bin/bash

if [ "$HOSTNAME" = "liftbridge-0" ]
then
  echo "Running in liftbridge-0 as a bootstrap seed...\n"
  liftbridge --data-dir=/data --config /etc/liftbridge.conf --level=debug --nats-servers=nats://nats.liftbridge.svc:4222 --raft-bootstrap-seed --id="$HOSTNAME"
else
  echo "Running in $HOSTNAME...\n"
  liftbridge --data-dir=/data --config /etc/liftbridge.conf --level=debug --nats-servers=nats://nats.liftbridge.svc:4222 --id="$HOSTNAME"
fi
