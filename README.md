<!--
  Licensed to the Apache Software Foundation (ASF) under one  
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information       
  regarding copyright ownership.  The ASF licenses this file  
  to you under the Apache License, Version 2.0 (the           
  "License"); you may not use this file except in compliance  
  with the License.  You may obtain a copy of the License at  
                                                              
    http://www.apache.org/licenses/LICENSE-2.0                
                                                              
  Unless required by applicable law or agreed to in writing,  
  software distributed under the License is distributed on an 
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY      
  KIND, either express or implied.  See the License for the   
  specific language governing permissions and limitations     
  under the License.                                             
-->

[![Build Status](https://travis-ci.org/apache/incubator-samoa.svg?branch=master)](https://travis-ci.org/apache/incubator-samoa)

Apache SAMOA: Scalable Advanced Massive Online Analysis.
=================
Apache SAMOA is a platform for mining on big data streams.
It is a distributed streaming machine learning (ML) framework that contains a 
programing abstraction for distributed streaming ML algorithms.

Apache SAMOA enables development of new ML algorithms without dealing with 
the complexity of underlying streaming processing engines (SPE, such 
as Apache Storm and Apache S4). Apache SAMOA also provides extensibility in integrating
new SPEs into the framework. These features allow Apache SAMOA users to develop 
distributed streaming ML algorithms once and to execute the algorithms 
in multiple SPEs, i.e., code the algorithms once and execute them in multiple SPEs.

## Build

### Storm mode

Simply clone the repository and install SAMOA.

```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -Pstorm package
```

The deployable jar for SAMOA will be in `target/SAMOA-Storm-0.3.0-SNAPSHOT.jar`.

### S4 mode

If you want to compile SAMOA for S4, you will need to install the S4 dependencies
manually as explained in [Executing SAMOA with Apache S4](http://samoa.incubator.apache.org/documentation/Executing-SAMOA-with-Apache-S4.html).

Once the dependencies if needed are installed, you can simply clone the repository and install SAMOA.

```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -Ps4 package
```

### Apex mode

Simply clone the repository and and create SAMOA with Apex package.
```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -Papex package
```

### Local mode

If you want to test SAMOA in a local environment, simply clone the repository and install SAMOA.

```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn package
```

The deployable jar for SAMOA will be in `target/SAMOA-Local-0.3.0-SNAPSHOT.jar`.

## Documentation

The documentation is intended to give an introduction on how to use Apache SAMOA in the various possible ways. 
As a user you can use it to develop new algorithms and test different Distributed Stream Processing Engines.

[Documentation](http://samoa.incubator.apache.org/documentation/Home.html)

[Javadoc](http://samoa.incubator.apache.org/docs/api/)

## Slides

[![SAMOA Slides](http://samoa.incubator.apache.org/samoa-slides.jpg)](https://speakerdeck.com/gdfm/samoa-a-platform-for-mining-big-data-streams)

G. De Francisci Morales, A. Bifet [SAMOA: Scalable Advanced Massive Online Analysis](http://jmlr.csail.mit.edu/papers/volume16/morales15a/morales15a.pdf)
Journal of Machine Learning Research, 16(Jan):149−153, 2015.

## Apache SAMOA Developer's Guide

<p><a href="https://samoa.incubator.apache.org/documentation/SAMOA-Developers-Guide-0.0.1.pdf"><img style="max-width:95%;border:3px solid black;" src="http://samoa.incubator.apache.org/Manual.png" alt="SAMOA Developer's Guide" height="250"> </a></p>

## Contributors
[List of contributors to the Apache SAMOA project](http://samoa.incubator.apache.org/documentation/Team.html)

## License

The use and distribution terms for this software are covered by the
Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html).

