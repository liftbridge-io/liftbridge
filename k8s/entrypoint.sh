#!/bin/bash

if [[ "$HOSTNAME" =~ -0$ ]];
then
  echo "Running in liftbridge-0 as a bootstrap seed..."
  liftbridge --data-dir=/data --config /etc/liftbridge.conf --raft-bootstrap-seed --id="$HOSTNAME"
else
  echo "Running in $HOSTNAME..."
  liftbridge --data-dir=/data --config /etc/liftbridge.conf --id="$HOSTNAME"
fi
