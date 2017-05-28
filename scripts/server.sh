#!/bin/bash

SCRIPT_DIR=`dirname $0`
SCRIPT_DIR=$(cd $SCRIPT_DIR; pwd)

##########################################
###### Determine cwmp Home Path
##########################################
export CWMP_HOME=`cd $SCRIPT_DIR/..; pwd`

##########################################
###### Determine Vertx Paths
##########################################
VERTX_RUNTIME_PATH=$CWMP_HOME/vertx-runtime
VERTX_ARTIFACTS_PATH=$CWMP_HOME/vertx-artifacts

##########################################
###### Default Module Version
##########################################
MOD_VERSION=$(cat $CWMP_HOME/conf/version.txt)

#############################################
###### Function for Printing Usages and quit.
#############################################
show_usage_and_exit() {
  echo "Usage: $0 (commands...)"
  echo "commands:"
  echo "  start  acs                Start ACS server"
  echo "  stop acs                  Stop ACS server"
  echo "  status acs                Show status of ACS server process"
  echo "  fetch acs                 Fetch latest ACS Server artifact (acs-server-$MOD_VERSION-mod.zip) from Bamboo"
  echo "  artifact-info acs         Show ACS Server Artifact Info (path and date/time)"
  echo "  start  cpe                Start CPE server"
  echo "  stop cpe                  Stop CPE server"
  echo "  status cpe                Show status of CPE server process"
  echo "  fetch cpe                 Fetch latest CPE Server artifact (cpe-server-$MOD_VERSION-mod.zip) from Bamboo"
  echo "  artifact-info cpe         Show CPE Server Artifact Info (path and date/time)"
  echo "  check-version [branch]    Check the latest version for the specified branch and update conf/version.txt"
  echo "                            Available branch names: master, dev, production"
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
elif [ "$1" == "fetch" ]; then
  ACTION=fetch
elif [ "$1" == "artifact-info" ]; then
  ACTION=artifact_info
elif [ "$1" == "check-version" ]; then
  ACTION=check-version
else
  show_usage_and_exit
fi

##########################################
###### Check for module name
##########################################
if [ "$2" == "cpe" ]; then
  MOD_NAME=cpe-server
elif [ "$2" == "acs" ]; then
  MOD_NAME=acs-server
elif [ "$ACTION" = "check-version" ]; then
  MOD_NAME=cpe-server
  if [ "$2" == "master" ]; then
    BRANCH=SXA
  elif [ "$2" == "dev" ]; then
    BRANCH=SXAMN
  elif [ "$2" == "production" ]; then
    BRANCH=SXACB
  else
    echo "Unknown Branch Name $2!"
    show_usage_and_exit
  fi
else
  show_usage_and_exit
fi

#############################################
###### Artifact Name
#############################################
ARTIFACT_NAME=$VERTX_ARTIFACTS_PATH/${MOD_NAME}-${MOD_VERSION}-mod.zip

##########################################
###### Add "conf" and "libs" to classpath
##########################################
export CLASSPATH=$CWMP_HOME/conf:$CWMP_HOME/libs/*

##########################################
###### Log File
##########################################
# Get current timestamp for archiving the log files
CURRENT_TIME=$(date +"%Y-%m-%d-%T")
LOG_DIR=$CWMP_HOME/logs/$MOD_NAME/$CURRENT_TIME
LOG_FILE=$LOG_DIR/$MOD_NAME.log
MONITOR_CLUSTER_LOG=$LOG_DIR/$MOD_NAME-monitor-cluster.log

##########################################
###### Functions to get PIDs
##########################################
get_server_pid(){
  echo `ps -ef | grep "$ARTIFACT_NAME" | grep -v monitor | grep -v grep | awk '{print $2}'`
}
get_mon_pid(){
  echo `ps -ef | grep "$ARTIFACT_NAME" | grep monitor | awk '{print $2}'`
}

##########################################
###### Get the PIDs
##########################################
monitor_pid=$(get_mon_pid)
server_pid=$(get_server_pid)

if [ "$ACTION" = "start" ]; then
  ##########################################
  ###### Action is "start"
  ##########################################

  ###############################################################
  ###### Only start a new instance if target proc is NOT running
  ###############################################################
  running=N
  if [ -n "$monitor_pid" ]; then
    running=Y
    echo "The $MOD_NAME monitor is already running! (pid: $monitor_pid)"
  fi
  if [ -n "$server_pid" ]; then
    running=Y
    echo "The $MOD_NAME is running! (pid: $server_pid)"
  fi
  if [ "$running" = "Y" ]; then
    exit 2
  fi

  ##########################################
  ###### Initialize System Env
  ##########################################
  DEFAULT_PROPERTIES=$CWMP_HOME/conf/CWMP-acs-default-properties.sh
  if [ -f $DEFAULT_PROPERTIES ]; then
    echo '########################################################################'
    echo ''
    echo "Initializing System Env Variables with default values from $DEFAULT_PROPERTIES..."
    cat $DEFAULT_PROPERTIES
    source $DEFAULT_PROPERTIES
    echo ''
  fi
  if [ "$CWMP_ACS_CUSTOM_PROPERTIES" == "" ]; then
    export CWMP_ACS_CUSTOM_PROPERTIES=$CWMP_HOME/conf/CWMP-acs-custom-properties.sh
  fi
  if [ -f $CWMP_ACS_CUSTOM_PROPERTIES ]; then
    echo "Initializing System Env Variables with custom values from $CWMP_ACS_CUSTOM_PROPERTIES..."
    cat $CWMP_ACS_CUSTOM_PROPERTIES
    source $CWMP_ACS_CUSTOM_PROPERTIES
    echo ''
  fi

  ##########################################
  ###### $CWMP_JBOSS_API_HOST must be set
  ##########################################
  if [ "$CWMP_JBOSS_API_HOST" == "" ]; then
    echo '#############################################################################'
    echo ''
    echo 'Please set system env CWMP_JBOSS_API_HOST to point to the SXA JBoss Host!!!!!!'
    echo ''
    echo '#############################################################################'
    exit 1
  fi

  ##########################################
  ###### Make Sure the artifact exists
  ##########################################
  VERTX_CMD="vertx runzip $ARTIFACT_NAME -cluster -cluster-host localhost"
  if [ -f $ARTIFACT_NAME ]; then
    echo '#################################################################################'
    echo ''
    echo "Found target vertx artifact $ARTIFACT_NAME."
  else
    echo '########################################################################'
    echo ''
    echo " Target Vertx Artifact $ARTIFACT_NAME does not exist!!!!!!"
    echo ''
    echo '########################################################################'
    exit 1
  fi

  ###############################################################
  ###### Export Java Options
  ###############################################################
  export JAVA_OPTS="-server\
     -Xms1g\
      -Xmx4g\
     -XX:MaxPermSize=256m\
     -XX:-UseGCOverheadLimit\
     -XX:+UseConcMarkSweepGC\
     -XX:+HeapDumpOnOutOfMemoryError\
     -Dorg.vertx.logger-delegate-factory-class-name=io.vertx.core.logging.impl.SLF4JLogDelegateFactory\
     -Djava.io.tmpdir=$VERTX_RUNTIME_PATH/$MOD_NAME/tmp\
     -DLOG_FILE=$LOG_FILE"

  ###############################################################
  ###### Start a new instance now
  ###############################################################
  echo ''
  echo "Starting $MOD_NAME with a monitor process (in $VERTX_RUNTIME_PATH/$MOD_NAME) ..."
  echo ''
  mkdir -p $VERTX_RUNTIME_PATH/$MOD_NAME/tmp
  cd $VERTX_RUNTIME_PATH/$MOD_NAME
  export LOG_FILE="$LOG_FILE"
  mkdir -p $LOG_DIR
  $SCRIPT_DIR/monitor.sh "$VERTX_CMD" >> $MONITOR_CLUSTER_LOG 2>&1 &

  sleep 1
  monitor_pid=$(get_mon_pid)
  server_pid=$(get_server_pid)

  if [ -z "$monitor_pid" ]; then
    echo "Failed to start monitor process for $MOD_NAME!"
  else
    echo "Started the monitor process for $MOD_NAME (pid $monitor_pid)."
  echo ''
  fi
  if [ -z "$server_pid" ]; then
    echo "Failed to start $MOD_NAME!"
  else
    echo "Started the $MOD_NAME (pid $server_pid ). Log file: $LOG_FILE."
    cd $CWMP_HOME/logs/$MOD_NAME/
    unlink current
    ln -s $LOG_DIR current
    echo ''
  fi

elif [ "$1" = "stop" ]; then
  ##########################################
  ###### Action is "stop"
  ##########################################

  if [ -n "$monitor_pid" ]; then
    kill $monitor_pid
    sleep 1
    if kill -0 $monitor_pid 2>/dev/null; then
      echo "Failed to stop monitor process $monitor_pid for $MOD_NAME!"
    else
      echo "Stopped the $MOD_NAME monitor process (pid $monitor_pid)."
      echo ''
    fi
  else
    echo "The $MOD_NAME monitor process is not running."
  fi

  if [ -n "$server_pid" ]; then
    if kill -0 $server_pid 2>/dev/null; then
      kill $server_pid
      sleep 2
      if kill -0 $server_pid 2>/dev/null; then
        echo "Failed to stop $MOD_NAME $server_pid"
      else
        sleep 2
        echo "Stopped the $MOD_NAME process (pid $server_pid)."
        echo ''

        # Cleanup the tmp dir
        echo "Cleaning up tmp dir ($VERTX_RUNTIME_PATH/$MOD_NAME/tmp) ..."
        echo ''
        rm -Rf $VERTX_RUNTIME_PATH/$MOD_NAME/tmp
      fi
    else
      sleep 2
      echo "Stopped the $MOD_NAME process (pid $server_pid)."
      echo ''

      # Cleanup the tmp dir
      echo "Cleaning up tmp dir ($VERTX_RUNTIME_PATH/$MOD_NAME/tmp) ..."
      echo ''
      rm -Rf $VERTX_RUNTIME_PATH/$MOD_NAME/tmp
  fi
  else
    echo "The $MOD_NAME is not running."
  fi

elif [ "$ACTION" = "status" ]; then
  ##########################################
  ###### Action is "status"
  ##########################################
  if [ -z "$monitor_pid" ]; then
    echo "The $MOD_NAME monitor process is not running"
  else
    echo "The $MOD_NAME monitor process is running (pid: $monitor_pid)."
  fi
  if [ -z "$server_pid" ]; then
    echo "The $MOD_NAME is not running."
  else
    echo "The $MOD_NAME is running (pid: $server_pid)."
  fi
elif [ "$ACTION" = "fetch" ]; then
  ##########################################
  ###### Action is "fetch"
  ##########################################
  if [[ $MOD_VERSION == *"DEV-"* ]]; then
    echo 'Fetching from DEV Branch...'
    BRANCH=SXAMN
  elif [[ $MOD_VERSION == *"SNAPSHOT"* ]]; then
    echo 'Fetching from Master Branch...'
    BRANCH=SXA
  else
    echo 'Fetching from Custom/Release Branch...'
    BRANCH=SXACB
  fi
  URL_PREFIX=http://172.23.43.106/browse/$BRANCH-CCNGAC/latestSuccessful/artifact/BUILD
  FILE_NAME=$MOD_NAME-$MOD_VERSION-mod.zip
  URL=$URL_PREFIX/$MOD_NAME-mod.zip/$FILE_NAME
  mkdir -p $VERTX_ARTIFACTS_PATH
  cd $VERTX_ARTIFACTS_PATH
  echo "wget -N $URL"
  wget -N $URL
elif [ "$ACTION" = "artifact_info" ]; then
  ##########################################
  ###### Action is "artifact_info"
  ##########################################
  echo "Full $MOD_NAME Artifact Path/Name:"
  echo "$ARTIFACT_NAME"
  echo "Bamboo Timestamp:"
  stat -c %y $ARTIFACT_NAME
elif [ "$ACTION" = "check-version" ]; then
  ##########################################
  ###### Action is "check-version"
  ##########################################
  echo "Check the latest version from Bamboo ($2 branch)..."
  URL=http://172.23.43.106/browse/$BRANCH-CCNGAC/latestSuccessful/artifact
  rawResult=$(curl -s $URL | grep acs-server-mod.zip)
  rawResult=${rawResult#*acs-server-mod.zip/acs-server-}
  rawResult=${rawResult%-mod.zip*}
  version=${rawResult%-mod.zip*}
  echo "version: $version"
  echo "Updating $CWMP_HOME/conf/version.txt with the new version..."
  rm $CWMP_HOME/conf/version.txt
  echo $version > $CWMP_HOME/conf/version.txt
fi
