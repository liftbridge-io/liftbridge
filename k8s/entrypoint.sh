#!/bin/bash

if [[ "$HOSTNAME" =~ -0$ ]];
CLUSTER_NAME=${CLUSTER_NAME:=cluster}
then
  echo "Running in liftbridge-0 as a bootstrap seed..."
  liftbridge --data-dir=/data --config /etc/liftbridge.conf --raft-bootstrap-seed --id="${CLUSTER_NAME}-$HOSTNAME"
else
  echo "Running in $HOSTNAME..."
  liftbridge --data-dir=/data --config /etc/liftbridge.conf --id="${CLUSTER_NAME}-$HOSTNAME"
fi
