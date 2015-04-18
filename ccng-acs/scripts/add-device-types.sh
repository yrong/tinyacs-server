#!/bin/bash

#############################################
###### Show Usage and Exit
#############################################
show-usage-and-exit() {
    echo "Usage: "
    echo "  $0  [org id]    Add device types for a given org id"
    echo "  $0  show-orgs   List all organizations that are entitled with SXA-CC"
    exit 1
}

#########################################################################
###### Function for Adding a new type. the "type" string is passed in $1
#########################################################################
add_a_new_type() {
  echo "Adding device type $1 for org id $ORG_ID..."

  ###########################################################
  ###### Call mongo shell to execute the insert command
  ###########################################################
  INSERT_CMD="db[\"sxacc-device-types\"].insert({\"_id\":\"$ORG_ID-$1\",\"orgId\":\"$1\",\"manufacturer\":\"Calix\",\"modelName\":\"$1\"})"
  mongo --quiet --eval $INSERT_CMD sxa
}

#############################################
###### Validate arguments
#############################################
if [ "$1" == "" ]; then
  show-usage-and-exit
elif [ "$1" == "show-orgs" ]; then
  LIST_CMD="printjson(db[\"sxacc-organizations\"].find().toArray())"
  mongo --quiet --eval $LIST_CMD sxa
  exit 1
fi


#############################################
###### Save org id
#############################################
ORG_ID=$1
add_a_new_type 844G-1
add_a_new_type 844G-2
add_a_new_type 854G-1
add_a_new_type 854G-2