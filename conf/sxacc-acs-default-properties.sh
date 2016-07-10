#################################################################################
### Export System Environment Variables for CPE Server Load Balancer Hostname/Port
#################################################################################
#export SXA_CPE_SERVER_LB_HOST=     (default to the IP address of the first non-loopback interface)
export SXA_CPE_SERVER_LB_PORT=8080
export SXA_CPE_SERVER_LB_HTTPS_ENABLED=false

##################################################################################
### System Environment Variables for ACS File Server Host/Ports/FileStorePath
##################################################################################
#export SXA_CC_ACS_FILE_SERVER_HOST=    (default to the IP address of the first non-loopback interface)
export SXA_CC_ACS_FILE_SERVER_DOWNLOAD_PORT=8021
export SXA_CC_ACS_FILE_SERVER_UPLOAD_PORT=8022
export SXA_CC_ACS_FILE_STORE_PATH=$SXACC_HOME/acs-file-store

#################################################################################
### System Environment Variables for SXA JBOSS API Server Hostname/Port
#################################################################################
if [ "$HOSTNAME" == "SXA1-CC-DEV" ]; then
    export SXA_JBOSS_API_HOST="199.71.143.184"
fi
export SXA_JBOSS_API_PORT=8080

#################################################################################
### System Environment Variables for MongoDB Host/Port/DB_Name
#################################################################################
export SXA_MONGO_HOST="localhost"
export SXA_MONGO_PORT=27017
export SXA_MONGO_DB_NAME="sxa"

#################################################################################
### System Environment Variables for Redis Host/Port/Timeout
#################################################################################
export SXA_REDIS_HOST="localhost"
export SXA_REDIS_PORT=6379
export SXA_REDIS_TIMEOUT=2000


#################################################################################
### System Environment Variables for max # of concurrent discovery sessions
#################################################################################
export SXA_CC_PERFORM_DEEP_DISCOVERY=false
export SXA_CC_MAX_DISCOVERY_SESSIONS=10
export SXA_CC_NBR_OF_PASSIVE_WORKFLOW_WORKER_VERTICES=2

#################################################################################
### System Environment Variables for max # of concurrent auto backup tasks (32)
### and soak time (30 mins)
#################################################################################
export SXA_CC_MAX_AUTO_BACKUP_TASKS=32
export SXA_CC_AUTO_BACKUP_SOAK_TIME=1800