#!/usr/bin/env bash
# 
# Runs a Cloudata command as a daemon.
#
# Environment Variables
#
#   CLOUDATA_CONF_DIR  Alternate conf dir. Default is ${CLOUDATA_HOME}/conf.
#   CLOUDATA_LOG_DIR   Where log files are stored.  PWD by default.
##

usage="Usage: cloudata-daemon.sh (start|stop) <command>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

if [ -f "${CLOUDATA_CONF_DIR}/cloudata-env.sh" ]; then
  . "${CLOUDATA_CONF_DIR}/cloudata-env.sh"
fi

startStop=$1
shift
command=$1
shift

cloudata_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ "$CLOUDATA_LOG_DIR" = "" ]; then
  export CLOUDATA_LOG_DIR="$CLOUDATA_HOME/logs"
fi
mkdir -p "$CLOUDATA_LOG_DIR"

if [ "$CLOUDATA_PID_DIR" = "" ]; then
  CLOUDATA_PID_DIR=/tmp
fi

if [ "$CLOUDATA_IDENT_STRING" = "" ]; then
  export CLOUDATA_IDENT_STRING="$USER"
fi

# some variables
export CLOUDATA_LOGFILE=cloudata-$CLOUDATA_IDENT_STRING-$command-`hostname`.log
export CLOUDATA_ROOT_LOGGER="INFO,DRFA"
log=$CLOUDATA_LOG_DIR/cloudata-$CLOUDATA_IDENT_STRING-$command-`hostname`.out
pid=$CLOUDATA_PID_DIR/cloudata-$CLOUDATA_IDENT_STRING-$command.pid

if [ ! -e $CLOUDATA_PID_DIR ]; then
    mkdir $CLOUDATA_PID_DIR
fi
    
# Set default scheduling priority
if [ "$CLOUDATA_NICENESS" = "" ]; then
    export CLOUDATA_NICENESS=0
fi

case $startStop in

  (start)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    cloudata_rotate_log $log
    echo starting $command, logging to $log
    nohup nice -n $CLOUDATA_NICENESS "$CLOUDATA_HOME"/bin/cloudata $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
