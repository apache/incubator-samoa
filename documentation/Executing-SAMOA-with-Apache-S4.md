---
title: Executing Apache SAMOA with Apache S4
layout: documentation
documentation: true
---

In this tutorial page we describe how to execute SAMOA on top of Apache S4.

## Prerequisites ##
The following dependencies are needed to run SAMOA smoothly on Apache S4

* [Gradle](http://www.gradle.org/)
* [Apache S4](https://incubator.apache.org/s4/)

## Gradle ##
Gradle is a build automation tool and is used to build Apache S4. The installation guide can be found [here.](http://www.gradle.org/docs/current/userguide/installation.html) The following instructions is a simplified installation guide.

1. Download Gradle binaries from [downloads](http://services.gradle.org/distributions/gradle-1.6-bin.zip), or from the console type `wget http://services.gradle.org/distributions/gradle-1.6-bin.zip`
1. Unzip the file `unzip gradle-1.6-bin.zip`
1. Set the Gradle environment variable: `export GRADLE_HOME=/foo/bar/gradle-1.6`
1. Add to the systems path `export PATH=$PATH:$GRADLE_HOME/bin`
1. Install Gradle by running `gradle`

Now you are all set to install Apache S4

## Apache S4 ##
S4 is a general-purpose, distributed, scalable, fault-tolerant, pluggable platform that allows programmers to easily develop applications for processing continuous unbounded streams of data. The installation process is as follows:

1. Download the latest Apache S4 release from [Apache S4 0.6.0](http://www.apache.org/dist/incubator/s4/s4-0.6.0-incubating/apache-s4-0.6.0-incubating-src.zip) or from command line `wget http://www.apache.org/dist/incubator/s4/s4-0.6.0-incubating/apache-s4-0.6.0-incubating-src.zip` or clone from git.
`git clone https://git-wip-us.apache.org/repos/asf/incubator-s4.git`.
1. Unzip the file `unzip apache-s4-0.6.0-incubating-src.zip` or go in the cloned directory.
1. Set the Apache S4 environment variable `export S4_HOME=/foo/bar/apache-s4-0.6.0-incubating-src`.
1. Add the S4_HOME to the system PATH. `export PATH=$PATH:$S4_HOME`.
1. Once the previous steps are done we can proceed to build and install Apache S4.
1. You can have a look at the available build tasks by typing `gradle tasks`.
1. There are some dependencies issues, therefore you should run the wrapper task first by typing `gradle wrapper`.
1. Install the artifacts for Apache S4 by running `gradle install` in the S4_HOME directory.
1. Install the S4-TOOLS, `gradle s4-tools::installApp`.

Done. Now you can configure and run your Apache S4 cluster.

***

## Building SAMOA ##
Once the S4 dependencies are installed, you can simply clone the repository and install SAMOA.

```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -Ps4 package 
```

The deployable jars for SAMOA will be in `target/SAMOA-<variant>-<version>-SNAPSHOT.jar`. For example, in our case for S4 `target/SAMOA-S4-0.3.0-SNAPSHOT.jar`.

***

## SAMOA-S4 Configuration ##
This section will go through the `bin/samoa-s4.properties` file and how to configure it.
In order for SAMOA to run correctly in a distributed environment there are some variables that need to be defined. Since Apache S4 uses [ZooKeeper](https://zookeeper.apache.org/) for cluster management we need to define where it is running.

    # Zookeeper Server
    zookeeper.server=localhost
    zookeeper.port=2181

Apache S4 also distributes the application via HTTP, therefore the server and port which contains the S4 application must be provided.

    # Simple HTTP Server providing the packaged S4 jar
    http.server.ip=localhost
    http.server.port=8000

Apache S4 uses the concept of logical clusters to define a group of machines, which are identified by an ID and start serving on a specific port.

    # Name of the S4 cluster
    cluster.name=cluster
    cluster.port=12000

SAMOA can be deployed on a single machine using only one resource or in a cluster environments. The following property can be defined to deploy as a `local` application or on a `cluster`.

    # Deployment strategy
    samoa.deploy.mode=local

***

## SAMOA S4 Deployment ##

In order to deploy SAMOA in a distributed environment you **MUST** configure the `bin/samoa-s4.properties` file correctly. If you are running locally it is optional to modify the properties file.

The deployment is done by running the SAMOA execution script `bin/samoa` with some additional parameters.
The execution syntax is as follows:
`bin/samoa <platform> <jar-location> <task & options>`

Example:
    
    bin/samoa S4 target/SAMOA-S4-0.0.1-SNAPSHOT.jar "ClusteringEvaluation"

The \<platform\> can be s4 or storm.

The \<jar-location\> must be the absolute path to the platform specific jar file.

The \<task & options\> should be the name of a known task and the options belonging to that task.

