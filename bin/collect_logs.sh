#!/usr/bin/env bash

BIN=`dirname "$0"`
CONF=`cd "$BIN/../conf"; pwd`

DIR_NAME="./slave_logs"

if [ ! -e $DIR_NAME ]
then
    mkdir $DIR_NAME
else
    rm -rf $DIR_NAME/*
fi

SLAVE_LIST=`cat $CONF/tablet_servers`
COMMITLOGSERVER_LIST=`cat $CONF/commitlog_servers`

LOG_PATH=$CLOUDATA_LOG_DIR

if [ "$LOG_PATH" = "" ]
then
	LOG_PATH=`cd "$BIN/../logs"; pwd`
fi

for HOST in $SLAVE_LIST
do
    scp $HOST:$LOG_PATH/*tabletserver*.log ./$DIR_NAME/
    scp $HOST:$LOG_PATH/*tabletserver*.out ./$DIR_NAME/
done

for HOST in $COMMITLOGSERVER_LIST
do
    scp $HOST:$LOG_PATH/*commitlogserver*.log ./$DIR_NAME/
    scp $HOST:$LOG_PATH/*commitlogserver*.out ./$DIR_NAME/
done
