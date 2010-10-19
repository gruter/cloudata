#!/usr/bin/env bash

# Start cloudata daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

exec "$bin/restserver.sh" cd "$CLOUDATA_HOME" \; "$bin/cloudata-daemon.sh" start restserver
