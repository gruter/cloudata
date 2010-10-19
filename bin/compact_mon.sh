bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

SLEEP=5

if [ $# -gt 0 ]
then
	SLEEP=$1
fi

while(true)
do
DATE=`date +%R:%S`
	$bin/cloudata pshell -listAll | awk -v date_var="$DATE" 'BEGIN{mc=0;sc=0;} /minor-compaction-lock/{mc++} /split-compaction-lock/{sc++} END{print date_var, ", minor compaction:", mc, ", split&major compaction:", sc}'
	sleep $SLEEP
done

