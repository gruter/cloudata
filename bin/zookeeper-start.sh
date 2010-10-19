#!/usr/bin/env bash

# Start cloudata daemons.  Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

if [ -f "${CLOUDATA_CONF_DIR}/cloudata-env.sh" ]; then
  . "${CLOUDATA_CONF_DIR}/cloudata-env.sh"
fi

. "$bin"/cloudata-config.sh

"$bin"/cloudata-daemon.sh start zookeeper ${ZOOKEEPER_CONF}
