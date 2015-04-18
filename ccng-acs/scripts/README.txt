This directory contains the scripts for deploying/starting/stopping/restarting SXA-CC Vert.x Modules (CPE server
and ACS Server).

The target deployment directory tree must be setup this way:

$SXACC_HOME
    |
    |
    ---- scripts
    |       |
    |       |
    |       ---- server.sh
    |            monitor.sh
    |
    ---- libs
    |       |
    |       |
    |       ---- logback-classic-1.1.2.jar
    |            logback-core-1.1.2.jar
    |            slf4j-api-1.6.2.jar
    |
    ---- conf
    |       |
    |       |
    |       ---- logback.xml
    |            sxacc-acs.properties
    |
    ---- vertx-artifacts
    |       |
    |       |
    |       ---- acs-server-0.0.1.DEV-SNAPSHOT-mod.zip
    |            cpe-server-0.0.1.DEV-SNAPSHOT-mod.zip
    |
    ---- vertx-runtime          (will be created automatically)
    |       |
    |       |
    |       ---- acs-server
    |            cpe-server
    |
    ---- logs                   (will be created automatically)
            |
            |
            ---- acs-server
                 cpe-server

The "scripts"/"conf"/"libs" directories are checked into the GIT repo, and shall be manually copied into the deployment
server.

The recommended root location is "/home/sxacc-dev" (for a dedicated user account "sxacc-dev").

The "vertx-artifacts" directory shall be created during installation, and the Vertx artifacts (the 2 zip files) shall be
manually copied into the "vertx-artifacts" directory.

Prior to starting ACS server and CPE server:
 1. MongoDB and Redis Servers must be installed and started in the same machine.
 2. The Linux System env variable "SXA_JBOSS_API_HOST" must be set properly to point the SXA JBOSS Host.

To start the ACS server, simply run the following shall command:

    scripts/server.sh start acs

To start the CPE server, simply run the following shall command:

    scripts/server.sh start cpe
