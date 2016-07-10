#!/bin/sh
./runAcs.sh stop cpe
echo "starting cpe server"
./runAcs.sh start cpe > logs/cpe.log 2>&1 &