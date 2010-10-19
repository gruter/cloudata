#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

HOSTLIST=${CLOUDATA_CONF_DIR}/tablet_servers
if [ ! -n "$1" ] 
then
  echo "usage: $0 \"command arg1 arg2 \""
  exit 1
fi

for i in `cat $HOSTLIST`
do
	echo "exec" $i "$*"
	ssh $i "$*"
done	