---
layout:     post
title:      "ACS CWMP Java Implementation"
subtitle:   "A blazing fast TR-069 auto configuration server (ACS) built with Vertx, Redis, and MongoDB. "
date:       2016-07-13 15:00:00
author:     "Ron"
header-img: "img/post-bg-kuaidi.jpg"
catalog: true
tags:
    - java
    - vertx
    - redis
    - mongodb
---

[ACS CWMP](https://www.broadband-forum.org/cwmp.php) Java Implementation
===============================
A blazing fast TR-069 auto configuration server (ACS) built with [Vertx](http://vertx.io/vertx2/), Redis, and MongoDB. I've added the source code in [github](https://github.com/yrong/tinyacs) and welcome comments.

## Installation Requirements

> [vertx](http://vertx.io/vertx2/install.html)
<br>
> [mongodb](https://docs.mongodb.com/manual/installation/)
<br>
> [redis](http://redis.io/topics/quickstart)
<br>


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





