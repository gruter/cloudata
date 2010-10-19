#!/usr/bin/env bash

BIN=`dirname "$0"`
CONF=`cd "$BIN/../conf"; pwd`

SLAVE_LIST=`cat $CONF/tablet_servers`
COMMITLOGSERVER_LIST=`cat $CONF/commitlog_servers`

LOG_PATH=$CLOUDATA_LOG_DIR

if [ "$LOG_PATH" = "" ]
then
	LOG_PATH=`cd "$BIN/../logs"; pwd`
fi

for HOST in $SLAVE_LIST
do
    echo ssh $HOST "rm -rf $LOG_PATH/*"
    ssh $HOST "rm -rf $LOG_PATH/*"
done

for HOST in $COMMITLOGSERVER_LIST
do
    echo ssh $HOST "rm -rf $LOG_PATH/*"
    ssh $HOST "rm -rf $LOG_PATH/*"
done
