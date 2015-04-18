#!/bin/bash

CMD=
PID=
LOG=

if [ $# -eq 0 ]; then
  echo "usage:  $0 [-o logfile] cmd"
  exit 2
fi

if [ "$1" = "-o" ]; then
  if [ $# -ne 3 ]; then
    echo "usage:  $0 [-o logfile] cmd"
    exit 2
  fi
  LOG=$2

  shift
  shift
else
  if [ $# -ne 1 ]; then
    echo "usage:  $0 [-o logfile] cmd"
    exit 2
  fi
fi

CMD=$1

echo "cmd: $CMD"
echo "log: $LOG"

start_cmd(){
  echo "`date`: starting process"
  if [ -z "$LOG" ]; then
    $CMD &
  else
    $CMD > $LOG 2>&1 &
  fi

  PID=$!
  echo "`date`: process [$PID] started"
}

sigterm_handle(){
  echo "`date`: SIGTERM received"
  keepLoop=false
  kill $PID
  exit 143
}

trap "sigterm_handle" TERM

keepLoop=true
while $keepLoop; do
  start_cmd

  echo "`date`: waiting process [$PID]"
  wait $PID
  RC=$?
  echo "`date`: process [$PID] returned with code: $RC"

  if [ $RC -eq 0 ]; then
    break
  fi
done

echo "`date`: monitor process exit"
exit 0
