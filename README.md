ACS CWMP(https://www.broadband-forum.org/cwmp.php) Java Implementation
===============================
A blazing fast TR-069 auto configuration server (ACS) built with Vertx(http://vertx.io/vertx2/), Redis, and MongoDB. 

## Installation Requirements

### install vertx(http://vertx.io/vertx2/install.html)
### install mongodb(https://docs.mongodb.com/manual/installation/)
### install redis(http://redis.io/topics/quickstart)


## Getting Started

### build from source
``` bash
$ mvn clean install
```

### Start Acs-Nbi Server
``` bash
$ sh restartNbi.sh
```
This is the northbound interface module. It exposes a REST API on port 8081 by default(see conf/cwmp-acs-default-properties.sh). This is needed for the GUI front end to communicate with.

### Start Acs-Cpe Server
``` bash
$ sh restartCpe.sh
```
This is the service that the CPEs will communicate with. It listens to port 8080 by default (see conf/cwmp-acs-default-properties.sh). Configure the ACS URL of your devices accordingly.

### Start Cpe Simulator
``` bash
$ sh restartSim.sh
```
This is the service that simulate a CPE 





