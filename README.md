ACS CWMP协议基于异步开源框架vertx的实现
===============================
A blazing fast [TR-069](https://www.broadband-forum.org/cwmp.php) auto configuration server (ACS) built with [Vertx](http://vertx.io/vertx2/), Redis, and MongoDB. 

## Infrastructure and message flow for nbi request 

![](/image/cwmp.png)

1. meteor stack send nbi request to acs nbi server by standard restful api and nbi server forward it to one cpe server by vertx eventbus api(eventbus send will choose one cpe server with round-robin algrithom)
2. cpe server will send connection request to the target cpe
3. cpe server will also store the nbi request into redis server
4. the cpe will send inform to the cpe server load balance to start the cwmp session
5. the cpe server load balance will send the inform to one cpe server by eventbus send api  
	5.1 the cpe server here may not be the cpe server in step 2   
	5.2 the cpe server will store it's identifier and the cpe's identifier into http cookie so that next time　load balance will sticky forward cwmp message from cpe to the same cpe server and cpe server will find cwmp session with that cpe
6. the cpe server will retrieve the nbi request from redis and translate it into cwmp request(s)
7. cpe server will send cwmp request to load balance
8. load balance send cwmp request to cpe
9. cpe send cwmp response back to load balance
10. load balance send cwmp response to cpe server
11. cpe server translate cwmp response to nbi response and send it back to nbi server

## Installation Requirements

- [vertx](http://vertx.io/vertx2/install.html)
nodejs on jvm

- [mongodb](https://docs.mongodb.com/manual/installation/)
document based nosql db

- [redis](http://redis.io/topics/quickstart)
small and fast db which support different data structures

- [meteor](https://www.meteor.com/install)
most efficient way to build javascript apps

## Build and Start

### Back end

+ build from source

``` bash
git clone https://github.com/yrong/tinyacs tinyacs
$ cd tinyacs && mvn clean install
```

+ Start Acs-Nbi Server

This is the northbound interface module. It exposes a REST API on port 8081 by default(see conf/cwmp-acs-default-properties.sh). This is needed for the GUI front end to communicate with.

``` bash
$ sh restartNbi.sh
```


+ Start Acs-Cpe Server

This is the service that the CPEs will communicate with. It listens to port 8080 by default (see conf/cwmp-acs-default-properties.sh). Configure the ACS URL of your devices accordingly.

``` bash
$ sh restartCpe.sh
```


+ Start Cpe Simulator

This is the service that simulate a CPE 

``` bash
$ sh restartSim.sh
```

### Front end

The front end is in another [repo](https://github.com/yrong/tinyacs-ui)　built with meteor framework and will talk to Acs-Nbi Server with rest api interface

``` bash
git clone https://github.com/yrong/tinyacs-ui tinyacs-ui
$ cd tinyacs-ui && MONGO_URL=mongodb://localhost:27017/cwmp meteor
```




