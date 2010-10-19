#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

# run zookeeper
#"$bin"/zookeeper-start.sh

# release zk memory lock
"$bin"/cloudata cloudatamaster -releaseLock

if [ -f "${CLOUDATA_CONF_DIR}/cloudata-env.sh" ]; then
  . "${CLOUDATA_CONF_DIR}/cloudata-env.sh"
fi

#delete all lock
if [ "$#" != "0" ]; then
  if [ $1 = "-format" ]; then
    "$bin"/cloudata cloudatamaster -formatLock
	elif [ $1 = "-format-f" ]; then
	  "$bin"/cloudata cloudatamaster -formatLock -noPrompt
  fi
else
  sleep 10
fi

#run commit logs servers
"$bin"/commitlog-start.sh

sleep 2

#delete commitlog/data file
if [ "$#" != "0" ]; then
  if [ $1 = "-format" -o $1 = "-format-f" ]; then
    "$bin"/cloudata cloudatamaster -formatFile -noPrompt
  fi
fi

#run CloudataMaster, TabletServer
"$bin"/cloudata-start.sh
