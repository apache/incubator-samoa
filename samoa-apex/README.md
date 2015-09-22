# Execution Instructions

This explains the build and run instructions for Samoa on Apache Apex (http://apex.apache.org/)

## Build

Simply clone the repository and and create SAMOA with Apex package.
```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn -Papex package
```

The deployable jar will be present in `target/SAMOA-Apex-0.4.0-incubating-SNAPSHOT.jar`.

## Running Samoa Algorithms on Apex in Local mode
- Edit samoa-apex.properties. Make sure `samoa.apex.mode` is set to `local`
- Run the deployable jar from the top level Samoa directory using the parameters for the algorithm. For example, for running the VHT classifier, run: `bin/samoa apex target/SAMOA-Apex-0.4.0-incubating-SNAPSHOT.jar "PrequentialEvaluation -d /tmp/dump.csv -i 500000 -l (classifiers.trees.VerticalHoeffdingTree -p 1) -s (generators.RandomTreeGenerator -c 2 -o 5 -u 5)"`

## Running Samoa Algorithms on Apex in Cluster mode
- A running Hadoop 2.0 (YARN) cluster is necessary for running Apex in cluster mode.
- Edit samoa-apex.properties.
 - Make sure `samoa.apex.mode` is set to `cluster`
 - Set the `dt.dfsRootDirectory` parameter to point to a valid HDFS directory
 - Set the `fs.default.name` parameter to point to the name node service of the Hadoop cluster
- Run the deployable jar from the top level Samoa directory using the parameters for the algorithm. For example, for running the VHT classifier, run: `bin/samoa apex target/SAMOA-Apex-0.4.0-incubating-SNAPSHOT.jar "PrequentialEvaluation -d /tmp/dump.csv -i 500000 -l (classifiers.trees.VerticalHoeffdingTree -p 1) -s (generators.RandomTreeGenerator -c 2 -o 5 -u 5)"`

## Notes on configuration
- Apex being a windowed stream processing engine, has certain limitations on the number of tuples that can be processed per second. This might require the user to limit the speed of tuples being sent by the Entrance Processing Item. The user can set the number of tuples per window (`tuplesLimitPerWindow` parameter) through the `dt-site.xml` config file. The path to this file needs to be set in `samoa-apex.properties` file as `dt.site.path`. Following is an example configuration which limits the speed to `2000` tuples per window. For Apex, with default steaming window size of `500` ms, this amounts to `4000` tuples per second speed.
This configuration parameter is specific to the type of data and the type of topology run and may not be optimal for all the topologies. A useful guide on choosing this parameter is to check if the latency of the operators in the Dag stays within a acceptable range. If not, the operator is not able to handle this load and this parameter must be decreased.

 ```
 <property>
   <name>dt.operator.*.prop.tuplesLimitPerWindow</name>
   <value>2000</value>
 </property>
 ```
 This will change the limit of all the operators present in the Dag. However, this will affect only input operators as other operators do not have this property. This is convenient as we don't need to know the name of the input operator corresponding to the Entrance Processing Item in the topology.

- Apart from the above configuration, Apex engine is highly configurable externally via the `dt-site.xml` file which is specified as the `dt.site.path` property in `samoa-apex.properties` file. Some of attributes which can be modified are as follows
 1. Memory allocated to an operator - `MEMORY_MB` http://docs.datatorrent.com/beginner/#allocating-operator-memory
 2. Size of the streaming window - `STREAMING_WINDOW_SIZE_MILLIS` http://docs.datatorrent.com/tutorials/topnwords-c7/#streaming-windows-and-application-windows
 3. Checkpoint window size - `CHECKPOINT_WINDOW_COUNT` https://apex.apache.org/docs/apex/application_development/#checkpointing

   Please refer the following for more information on what attributes can be specified externally and their impact on the processing engine. *However, note that most of these would not be applicable to applications running on Samoa, as the topology and its properties are already defined specified by Samoa. The Apex runner is for running the topology by converting it to an Apex Dag.*
   - https://www.datatorrent.com/docs/apidocs/com/datatorrent/api/Context.DAGContext.html
   - https://www.datatorrent.com/docs/apidocs/com/datatorrent/api/Context.OperatorContext.html

- To enable debug logging, add the following configuration to the `dt-site.xml` file specified at location in `dt.site.path`
 ```
 <property>
   <name>dt.loggers.level</name>
   <value>org.apache.*:DEBUG,com.datatorrent.*:DEBUG</value>
 </property>
 ```

 ## Using Apex cli
The user can view details about any application launched via Apex using the cli. The apex-core project must be checked out to some directory.
Launch the apex cli located at: ```apex-core/engine/src/main/scripts/apex```
Following can be achieved using the cli
 - View running apps: ```list-apps```
 - View app info:
   - Connect to the running application using : ```connect <app-id>```
   - Run ```get-app-info <app id>```
