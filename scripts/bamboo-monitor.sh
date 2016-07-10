##########################################################################################
######
###### To run this script:
######
###### nohup bamboo-monitor.sh > null
##########################################################################################


#!/bin/bash

# Monitor Bamboo Builds and auto re-deploy new builds

SCRIPT_DIR=`dirname $0`
LOG_DIR="$SCRIPT_DIR/../logs/bamboo-monitor"
LOG_FILE="$LOG_DIR/bamboo-monitor.log"

##########################################################################################
###### Function for Downloading the artifact and re-deploy if newer builds were downloaded.
##########################################################################################
try_fetch() {
    # Get the current timestamp of the ACS-Server artifact
    oldTimestamp=$($SCRIPT_DIR/server.sh artifact-info acs)
    # Try to fetch it
    $SCRIPT_DIR/server.sh fetch acs > null
    # Get the timestamp again
    newTimestamp=$($SCRIPT_DIR/server.sh artifact-info acs)

    # Compare the 2 timestamps
    if [[ "$oldTimestamp" == "$newTimestamp" ]]; then
        echo "`date`: No new build found."
    else
        echo "`date`: New ACS Server Build has been downloaded." > $LOG_FILE
        echo "`date`: Downloading new CPE Server build..." > $LOG_FILE
        $SCRIPT_DIR/server.sh fetch cpe
        echo "`date`: Restarting..." > $LOG_FILE
        nohup $SCRIPT_DIR/restart.sh > $LOG_FILE
    fi
}

#############################################
###### Try fetching in a loop
#############################################
while true; do
    try_fetch
    sleep 60
done
