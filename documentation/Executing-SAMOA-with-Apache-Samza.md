---
title: Executing Apache SAMOA with Apache Samza
layout: documentation
documentation: true
---
This tutorial describes how to run SAMOA on Apache Samza.
The steps included in this tutorial are:

1. Setup and configure a cluster with the required dependencies. This applies for single-node (local) execution as well.

2. Build SAMOA deployables

3. Configure SAMOA-Samza

4. Deploy SAMOA-Samza and execute a task

5. Observe the execution and the result

## Setup cluster
The following are needed to to run SAMOA on top of Samza:

* [Apache Zookeeper](http://zookeeper.apache.org/)
* [Apache Kafka](http://kafka.apache.org/)
* [Apache Hadoop YARN and HDFS](http://hadoop.apache.org/docs/r2.2.0/hadoop-yarn/hadoop-yarn-site/YARN.html)

### Zookeeper
Zookeeper is used by Kafka to coordinate its brokers. The detail instructions to setup a Zookeeper cluster can be found [here](http://zookeeper.apache.org/doc/trunk/zookeeperStarted.html). 

To quickly setup a single-node Zookeeper cluster:

1. Download the binary release from the [release page](http://zookeeper.apache.org/releases.html).

2. Untar the archive
 
```
tar -xf $DOWNLOAD_DIR/zookeeper-3.4.6.tar.gz -C ~/
```

3. Copy the default configuration file

```
cp zookeeper-3.4.6/conf/zoo_sample.cfg zookeeper-3.4.6/conf/zoo.cfg
```

4. Start the single-node cluster

```
~/zookeeper-3.4.6/bin/zkServer.sh start
```

### Kafka
Kafka is a distributed, partitioned, replicated commit log service which Samza uses as its default messaging system. 

1. Download a binary release of Kafka [here](http://kafka.apache.org/downloads.html). As mentioned in the page, the Scala version does not matter. However, 2.10 is recommended as Samza has recently been moved to Scala 2.10.

2. Untar the archive 

```
tar -xzf $DOWNLOAD_DIR/kafka_2.10-0.8.1.tgz -C ~/
```

If you are running in local mode or a single-node cluster, you can now start Kafka with the command:

```
~/kafka_2.10-0.8.1/bin/kafka-server-start.sh kafka_2.10-0.8.1/config/server.properties
```

In multi-node cluster, it is typical and convenient to have a Kafka broker on each node (although you can totally have a smaller Kafka cluster, or even a single-node Kafka cluster). The number of brokers in Kafka cluster will affect disk bandwidth and space (the more brokers we have, the higher value we will get for the two). In each node, you need to set the following properties in `~/kafka_2.10-0.8.1/config/server.properties` before starting Kafka service.

``` 
broker.id=a-unique-number-for-each-node
zookeeper.connect=zookeeper-host0-url:2181[,zookeeper-host1-url:2181,...]
```

You might want to change the retention hours or retention bytes of the logs to avoid the logs size from growing too big.

```
log.retention.hours=number-of-hours-to-keep-the-logs
log.retention.bytes=number-of-bytes-to-keep-in-the-logs
```

### Hadoop YARN and HDFS
> Hadoop YARN and HDFS are **not** required to run SAMOA in Samza local mode. 

To set up a YARN cluster, first download a binary release of Hadoop [here](http://www.apache.org/dyn/closer.cgi/hadoop/common/) on each node in the cluster and untar the archive
`tar -xf $DOWNLOAD_DIR/hadoop-2.2.0.tar.gz -C ~/`. We have tested SAMOA with Hadoop 2.2.0 but Hadoop 2.3.0 should work too.

**HDFS**

Set the following properties in `~/hadoop-2.2.0/etc/hadoop/hdfs-site.xml` in all nodes.

```
<configuration>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/username/hadoop-2.2.0/hdfs/datanode</value>
    <description>Comma separated list of paths on the local filesystem of a DataNode where it should store its blocks.</description>
  </property>
 
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/username/hadoop-2.2.0/hdfs/namenode</value>
    <description>Path on the local filesystem where the NameNode stores the namespace and transaction logs persistently.</description>
  </property>
</configuration>
```

Add this property in `~/hadoop-2.2.0/etc/hadoop/core-site.xml` in all nodes.

```
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000/</value>
    <description>NameNode URI</description>
  </property>

  <property>
    <name>fs.hdfs.impl</name>
    <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
  </property>
</configuration>
```
For a multi-node cluster, change the hostname ("localhost") to the correct host name of your namenode server.

Format HDFS directory (only perform this if you are running it for the very first time)

```
~/hadoop-2.2.0/bin/hdfs namenode -format
```

Start namenode daemon on one of the node

```
~/hadoop-2.2.0/sbin/hadoop-daemon.sh start namenode
```

Start datanode daemon on all nodes

```
~/hadoop-2.2.0/sbin/hadoop-daemon.sh start datanode
```

**YARN**

If you are running in multi-node cluster, set the resource manager hostname in `~/hadoop-2.2.0/etc/hadoop/yarn-site.xml` in all nodes as follow:

```
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>resourcemanager-url</value>
    <description>The hostname of the RM.</description>
  </property>
</configuration>
```

**Other configurations**
Now we need to tell Samza where to find the configuration of YARN cluster. To do this, first create a new directory in all nodes:

```
mkdir ~/.samza
mkdir ~/.samza/conf
```

Copy (or soft link) `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml` in `~/hadoop-2.2.0/etc/hadoop` to the new directory 

```
ln -s ~/.samza/conf/core-site.xml ~/hadoop-2.2.0/etc/hadoop/core-site.xml
ln -s ~/.samza/conf/hdfs-site.xml ~/hadoop-2.2.0/etc/hadoop/hdfs-site.xml
ln -s ~/.samza/conf/yarn-site.xml ~/hadoop-2.2.0/etc/hadoop/yarn-site.xml
```

Export the enviroment variable YARN_HOME (in ~/.bashrc) so Samza knows where to find these YARN configuration files.

```
export YARN_HOME=$HOME/.samza
```

**Start the YARN cluster**
Start resource manager on master node

```
~/hadoop-2.2.0/sbin/yarn-daemon.sh start resourcemanager
```

Start node manager on all worker nodes

```
~/hadoop-2.2.0/sbin/yarn-daemon.sh start nodemanager
```

## Build SAMOA
Perform the following step on one of the node in the cluster. Here we assume git and maven are installed on this node.

Since Samza is not yet released on Maven, we will have to clone Samza project, build and publish to Maven local repository:

```
git clone -b 0.7.0 https://github.com/apache/incubator-samza.git
cd incubator-samza
./gradlew clean build
./gradlew publishToMavenLocal
```
 
Here we cloned and installed Samza version 0.7.0, the current released version (July 2014). 

Now we can clone the repository and install SAMOA.

```
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -Psamza package
```

The deployable jars for SAMOA will be in `target/SAMOA-<variant>-<version>-SNAPSHOT.jar`. For example, in our case for Samza `target/SAMOA-Samza-0.2.0-SNAPSHOT.jar`.

## Configure SAMOA-Samza execution
This section explains the configuration parameters in `bin/samoa-samza.properties` that are required to run SAMOA on top of Samza.

**Samza execution mode**

```
samoa.samza.mode=[yarn|local]
```
This parameter specify which mode to execute the task: `local` for local execution and `yarn` for cluster execution.

**Zookeeper**

```
zookeeper.connect=localhost
zookeeper.port=2181
```
The default setting above applies for local mode execution. For cluster mode, change `zookeeper.host` to the correct URL of your zookeeper host.

**Kafka**

```
kafka.broker.list=localhost:9092
```
`kafka.broker.list` is a comma separated list of host:port of all the brokers in Kafka cluster.

```
kafka.replication.factor=1
```
`kafka.replication.factor` specifies the number of replicas for each stream in Kafka. This number must be less than or equal to the number of brokers in Kafka cluster.

**YARN**
> The below settings do not apply for local mode execution, you can leave them as they are.

`yarn.am.memory` and `yarn.container.memory` specify the memory requirement for the Application Master container and the worker containers, respectively. 

```
yarn.am.memory=1024
yarn.container.memory=1024
```

`yarn.package.path` specifies the path (typically a HDFS path) of the package to be distributed to all YARN containers to execute the task.

```
yarn.package.path=hdfs://samoa/SAMOA-Samza-0.2.0-SNAPSHOT.jar
```

**Samza**
`max.pi.per.container` specifies the number of PI instances allowed in one YARN container. 

```
max.pi.per.container=1
```

`kryo.register.file` specifies the registration file for Kryo serializer.

```
kryo.register.file=samza-kryo
```

`checkpoint.commit.ms` specifies the frequency for PIs to commit their checkpoints (in ms). The default value is 1 minute.

```
checkpoint.commit.ms=60000
```

## Deploy SAMOA-Samza task
Execute SAMOA task with the following command:

```
bin/samoa samza target/SAMOA-Samza-0.2.0-SNAPSHOT.jar "<task> & <options>" 
```

## Observe execution and result
In local mode, all the log will be printed out to stdout. If you execute the task on YARN cluster, the output is written to stdout files in YARN's containers' log folder ($HADOOP_HOME/logs/userlogs/application_\<application-id\>/container_\<container-id\>).
