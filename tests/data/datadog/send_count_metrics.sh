#!/bin/sh

SOCKET_PATH=$1;

echo -n "foo.metric.name:1|c" | nc -U -u -w1 ${SOCKET_PATH}
