#!/bin/bash

CLUSTER_NAME=${CLUSTER_NAME:=cluster}

if [[ "$HOSTNAME" =~ -0$ ]] && [[ "$SKIP_SEED" != "true" ]];
then
  echo "Running in $HOSTNAME as a bootstrap seed..."
  LIFTBRIDGE_HOST=$(hostname -i) liftbridge --data-dir=/data --config /etc/liftbridge.yaml --raft-bootstrap-seed --id="${CLUSTER_NAME}-$HOSTNAME"
else
  echo "Running in $HOSTNAME..."
  LIFTBRIDGE_HOST=$(hostname -i) liftbridge --data-dir=/data --config /etc/liftbridge.yaml --id="${CLUSTER_NAME}-$HOSTNAME"
fi
