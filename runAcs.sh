#!/bin/bash
versions=($(cat pom.xml | grep -oP '(?<=version>)[^<]+'))
MOD_VERSION=${versions[1]}

#############################################
###### Function for Printing Usages and quit.
#############################################
show_usage_and_exit() {
  echo "Usage: $0 (commands...)"
  echo "commands:"
  echo "  start  acs   Start ACS server"
  echo "  stop acs     Stop ACS server"
  echo "  status acs   Show status of ACS server"
  echo "  start  cpe   Start CPE server"
  echo "  stop cpe     Stop CPE server"
  echo "  status cpe   Show status of CPE server"
  exit 1
}

##########################################
###### Check for action
##########################################
if [ "$1" == "start" ]; then
  ACTION=start
elif [ "$1" == "stop" ]; then
  ACTION=stop
elif [ "$1" == "status" ]; then
  ACTION=status
else
  show_usage_and_exit
fi

##########################################
###### Check for module name
##########################################
if [ "$2" == "cpe" ]; then
  MOD_NAME=cpe-server
  DEBUG_PORT=9092
elif [ "$2" == "acs" ]; then
  MOD_NAME=acs-server
  DEBUG_PORT=9091
elif [ "$2" == "sim" ]; then
  MOD_NAME=cpe-sim
  DEBUG_PORT=9093  
else
  show_usage_and_exit
fi





##########################################
###### Functions to get PIDs
##########################################
get_server_pid(){
  echo `ps -ef | grep "$MOD_NAME" | grep -v monitor | grep -v grep | awk '{print $2}'`
}

##########################################
###### Get the PIDs
##########################################
server_pid=$(get_server_pid)

if [ "$1" = "start" ]; then
  ##########################################
  ###### Action is "start"
  ##########################################
  shift

  ###############################################################
  ###### Only start a new instance if target proc is NOT running
  ###############################################################
  running=N
  if [ -n "$server_pid" ]; then
    running=Y
    echo "The $MOD_NAME is running! (pid: $server_pid)"
  fi
  if [ "$running" = "Y" ]; then
    exit 2
  fi

  
##########################################
  ###### copy dependency
  ##########################################
  cp -r mods $MOD_NAME/target
  

  ###############################################################
  ###### Start a new instance now
  ###############################################################
  echo ''
  echo "Starting $MOD_NAME ..."
  echo ''
  
  cd $MOD_NAME/target
  export VERTX_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=$DEBUG_PORT"
  vertx runmod vertx2~$MOD_NAME~$MOD_VERSION -cluster -cluster-host localhost
  #vertx runzip $MOD_NAME-$MOD_VERSION-mod.zip
  
  sleep 1
  server_pid=$(get_server_pid)

  if [ -z "$server_pid" ]; then
    echo "Failed to start $MOD_NAME!"
  else
    echo "Started the $MOD_NAME has been (pid $server_pid )."
    echo ''
  fi

elif [ "$1" = "stop" ]; then
  ##########################################
  ###### Action is "stop"
  ##########################################
  shift


  if [ -n "$server_pid" ]; then
    if kill -0 $server_pid 2>/dev/null; then
      kill $server_pid
      sleep 2
      if kill -0 $server_pid 2>/dev/null; then
        echo "Failed to stop $MOD_NAME $server_pid"
      else
        sleep 2
        echo "Stopped the $MOD_NAME process (pid $server_pid)"
        echo ''
      fi
    else
      sleep 2
        echo "Stopped the $MOD_NAME process (pid $server_pid)"
        echo ''
    fi
  else
    echo "The $MOD_NAME is not running."
  fi

elif [ "$1" = "status" ]; then
  ##########################################
  ###### Action is "status"
  ##########################################
  shift
  if [ -z "$server_pid" ]; then
    echo "The $MOD_NAME is not running."
  else
    echo "The $MOD_NAME is running (pid: $server_pid)."
  fi

  
fi