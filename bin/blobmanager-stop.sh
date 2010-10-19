#!/usr/bin/env bash

# Stop cloudata-fs web server.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cloudata-config.sh

"$bin"/cloudata-daemon.sh stop blobmanager
