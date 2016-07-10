#!/bin/sh
./runAcs.sh stop sim
echo "starting simulator"
./runAcs.sh start sim > logs/sim.log 2>&1 &