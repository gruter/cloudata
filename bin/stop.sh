#!/usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

"$bin"/blobmanager-stop.sh
"$bin"/cloudatafs-stop.sh
"$bin"/commitlog-stop.sh
"$bin"/cloudata-stop.sh
#"$bin"/zookeeper-stop.sh
