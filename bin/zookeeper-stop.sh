#!/usr/bin/env bash

# Start cloudata daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

"$bin"/cloudata-daemon.sh stop zookeeper
