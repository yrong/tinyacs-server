#!/bin/sh
./runAcs.sh stop acs
echo "starting nbi server"
./runAcs.sh start acs > logs/nbi.log 2>&1 &