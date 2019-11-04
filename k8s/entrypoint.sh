#!/bin/bash

CLUSTER_NAME=${CLUSTER_NAME:=cluster}

if [[ "$HOSTNAME" =~ -0$ ]] && [[ "$SKIP_SEED" != "true" ]];
then
  echo "Running in liftbridge-0 as a bootstrap seed..."
  liftbridge --data-dir=/data --config /etc/liftbridge.conf --raft-bootstrap-seed --id="${CLUSTER_NAME}-$HOSTNAME"
else
  echo "Running in $HOSTNAME..."
  liftbridge --data-dir=/data --config /etc/liftbridge.conf --id="${CLUSTER_NAME}-$HOSTNAME"
fi
