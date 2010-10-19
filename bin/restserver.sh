#!/usr/bin/env bash
#
# Run a shell command on all slave hosts.
#
# Environment Variables
#
#   CLOUDATA_REST_SLAVES File naming remote hosts.
#     Default is ${CLOUDATA_CONF_DIR}/rest_servers.
#   CLOUDATA_CONF_DIR  Alternate conf dir. Default is ${CLOUDATA_HOME}/conf.
#   CLOUDATA_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   CLOUDATA_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: restserver.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

# If the slaves file is specified in the command line,
# then it takes precedence over the definition in
# cloudata-env.sh. Save it here.
HOSTLIST=$CLOUDATA_REST_SLAVES

if [ "$HOSTLIST" = "" ]; then
  if [ "$CLOUDATA_REST_SLAVES" = "" ]; then
    export HOSTLIST="${CLOUDATA_CONF_DIR}/rest_servers"
  else
    export HOSTLIST="${CLOUDATA_REST_SLAVES}"
  fi
fi

for slave in `cat "$HOSTLIST"`; do
 ssh $CLOUDATA_SSH_OPTS $slave $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
 if [ "$CLOUDATA_SLAVE_SLEEP" != "" ]; then
   sleep $CLOUDATA_SLAVE_SLEEP
 fi
done

wait
