#!/usr/bin/env bash

# Stop blobmerger

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

"$bin"/cloudata-daemon.sh stop cloudatafs
