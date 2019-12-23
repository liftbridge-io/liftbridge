#!/bin/sh

while ! nc -z $WAIT_FOR_HOST $WAIT_FOR_PORT; do sleep 1 ; echo waiting ; done

exec "$@"
