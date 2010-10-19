#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

HOSTLIST=${CLOUDATA_CONF_DIR}/tablet_servers
if [ 2 -gt "$#" ]
then
	echo "usage: $0 \"local_file remote_file\""
  exit 1
fi


for i in `cat $HOSTLIST`
do
	echo $i
	scp  "$1" $i:"$2"
done	
