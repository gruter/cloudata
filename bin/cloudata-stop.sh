#!/usr/bin/env bash

# Start cloudata daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

# start cloudata daemons
# start namenode after datanodes, to minimize time namenode is up w/o data
# note: datanodes will log connection errors until namenode starts
"$bin"/cloudata-daemon.sh stop cloudatamaster
exec "$bin/tabletservers.sh" cd "$CLOUDATA_HOME" \; "$bin/cloudata-daemon.sh" stop tabletserver
