---
title: Executing Apache SAMOA with Apache Storm
layout: documentation
documentation: true
---
In this tutorial page we describe how to execute SAMOA on top of Apache Storm. Here is an outline of what we want to do:

1. Ensure that you have necessary Storm cluster and configuration to execute SAMOA
2. Ensure that you have all the SAMOA deployables for execution in the cluster
3. Configure samoa-storm.properties
4. Execute SAMOA classification task
5. Observe the task execution

### Storm Configuration
Before we start the tutorial, please ensure that you already have Storm cluster (preferably Storm 0.8.2) running. You can follow this [tutorial](http://www.michael-noll.com/tutorials/running-multi-node-storm-cluster/) to set up a Storm cluster.

You also need to install Storm at the machine where you initiate the deployment, and configure Storm (at least) with this configuration in `~/.storm/storm.yaml`:

```
########### These MUST be filled in for a storm configuration
nimbus.host: "<enter your nimbus host name here>"

## List of custom serializations
kryo.register:
    - com.yahoo.labs.samoa.learners.classifiers.trees.AttributeContentEvent: com.yahoo.labs.samoa.learners.classifiers.trees.AttributeContentEvent$AttributeCEFullPrecSerializer
    - com.yahoo.labs.samoa.learners.classifiers.trees.ComputeContentEvent: com.yahoo.labs.samoa.learners.classifiers.trees.ComputeContentEvent$ComputeCEFullPrecSerializer
```
<!--
Or, if you are using SAMOA with optimized VHT, you should use this following configuration file:
```
########### These MUST be filled in for a storm configuration
nimbus.host: "<enter your nimbus host name here>"

## List of custom serializations
kryo.register:
     - com.yahoo.labs.samoa.learners.classifiers.trees.NaiveAttributeContentEvent: com.yahoo.labs.samoa.classifiers.trees.NaiveAttributeContentEvent$NaiveAttributeCEFullPrecSerializer
     - com.yahoo.labs.samoa.learners.classifiers.trees.ComputeContentEvent: com.yahoo.labs.samoa.classifiers.trees.ComputeContentEvent$ComputeCEFullPrecSerializer
```
-->

Alternatively, if you don't have Storm cluster running, you can execute SAMOA with Storm in local mode as explained in section [samoa-storm.properties Configuration](#samoa-storm-properties).

### SAMOA deployables
There are three deployables for executing SAMOA on top of Storm. They are:

1. `bin/samoa` is the main script to execute SAMOA. You do not need to change anything in this script.
2. `target/SAMOA-Storm-x.x.x-SNAPSHOT.jar` is the deployed jar file. `x.x.x` is the version number of SAMOA. 
3. `bin/samoa-storm.properties` contains deployment configurations. You need to set the parameters in this properties file correctly. 

### <a name="samoa-storm-properties"> samoa-storm.properties Configuration</a>
Currently, the properties file contains two configurations:

1. `samoa.storm.mode` determines whether the task is executed locally (using Storm's `LocalCluster`) or executed in a Storm cluster. Use `local` if you want to test SAMOA and you do not have a Storm cluster for deployment. Use `cluster` if you want to test SAMOA on your Storm cluster.
2. `samoa.storm.numworker` determines the number of worker to execute the SAMOA tasks in the Storm cluster. This field must be an integer, less than or equal to the number of available slots in you Storm cluster. If you are using local mode, this property corresponds to the number of thread used by Storm's LocalCluster to execute your SAMOA task.

Here is the example of a complete properties file:

```
# SAMOA Storm properties file
# This file contains specific configurations for SAMOA deployment in the Storm platform
# Note that you still need to configure Storm client in your machine, 
# including setting up Storm configuration file (~/.storm/storm.yaml) with correct settings

# samoa.storm.mode corresponds to the execution mode of the Task in Storm 
# possible values:
#   1. cluster: the Task will be sent into nimbus. The nimbus is configured by Storm configuration file
#   2. local: the Task will be sent using local Storm cluster
samoa.storm.mode=cluster

# samoa.storm.numworker corresponds to the number of worker processes allocated in Storm cluster
# possible values: any integer greater than 0  
samoa.storm.numworker=7
```

### SAMOA task execution

You can execute a SAMOA task using the aforementioned `bin/samoa` script with this following format:
`bin/samoa <platform> <jar> "<task>"`.

`<platform>` can be `storm` or `s4`. Using `storm` option means you are deploying SAMOA on a Storm environment. In this configuration, the script uses the aforementioned yaml file (`~/.storm/storm.yaml`) and `samoa-storm.properties` to perform the deployment. Using `s4` option means you are deploying SAMOA on an Apache S4 environment. Follow this [link](Executing-SAMOA-with-Apache-S4) to learn more about deploying SAMOA on Apache S4.

`<jar>` is the location of the deployed jar file (`SAMOA-Storm-x.x.x-SNAPSHOT.jar`) in your file system. The location can be a relative path or an absolute path into the jar file. 

`"<task>"` is the SAMOA task command line such as `PrequentialEvaluation` or `ClusteringTask`. This command line for SAMOA task follows the format of [Massive Online Analysis (MOA)](http://moa.cms.waikato.ac.nz/details/classification/command-line/).

The complete command to execute SAMOA is:

```
bin/samoa storm target/SAMOA-Storm-0.0.1-SNAPSHOT.jar "PrequentialEvaluation -d /tmp/dump.csv -i 1000000 -f 100000 -l (com.yahoo.labs.samoa.learners.classifiers.trees.VerticalHoeffdingTree -p 4) -s (com.yahoo.labs.samoa.moa.streams.generators.RandomTreeGenerator -c 2 -o 10 -u 10)"
```
The example above uses [Prequential Evaluation task](Prequential-Evaluation-Task) and [Vertical Hoeffding Tree](Vertical-Hoeffding-Tree-Classifier) classifier. 

### Observing task execution
There are two ways to observe the task execution using Storm UI and by monitoring the dump file of the SAMOA task. Notice that the dump file will be created on the cluster if you are executing your task in `cluster` mode.

#### Using Storm UI
Go to the web address of Storm UI and check whether the SAMOA task executes as intended. Use this UI to kill the associated Storm topology if necessary.

#### Monitoring the dump file
Several tasks have options to specify a dump file, which is a file that represents the task output. In our example, [Prequential Evaluation task](Prequential-Evaluation-Task) has `-d` option which specifies the path to the dump file. Since Storm performs the allocation of Storm tasks, you should set the dump file into a file on a shared filesystem if you want to access it from the machine submitting the task.
