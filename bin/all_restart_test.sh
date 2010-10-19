#!/usr/bin/env bash

## This script is just for Hudson test

echo "KILL ALL THE JAVA PROCESSES EXCEPT HUDSON PROCESS"
ps -ef | grep "java" | grep -v "hudson" | grep -v "grep" | awk '{print $2}' | xargs kill -9

echo "SLEEP 2 SEC"
sleep 2

echo "RUN PLEIADES"
./bin/start-all.sh

echo "TEST IS READY..."
