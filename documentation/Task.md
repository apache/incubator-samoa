---
title: Task
layout: documentation
documentation: true
---
Task is similar to a job in Hadoop. Task is an execution entity. A topology must be defined inside a task. SAMOA can only execute classes that implement `Task` interface.

###1. Implementation
```
package com.yahoo.labs.samoa.tasks;

import com.yahoo.labs.samoa.topology.ComponentFactory;
import com.yahoo.labs.samoa.topology.Topology;

/**
 * Task interface, the mother of all SAMOA tasks!
 */
public interface Task {

	/**
	 * Initialize this SAMOA task, 
	 * i.e. create and connect Processors and Streams
	 * and initialize the topology
	 */
	public void init();	
	
	/**
	 * Return the final topology object to be executed in the cluster
	 * @return topology object to be submitted to be executed in the cluster
	 */
	public Topology getTopology();
	
	/**
	 * Sets the factory.
	 * TODO: propose to hide factory from task, 
	 * i.e. Task will only see TopologyBuilder, 
	 * and factory creation will be handled by TopologyBuilder
	 *
	 * @param factory the new factory
	 */
	public void setFactory(ComponentFactory factory) ;
}
```

###2. Methods
#####2.1 `void init()`
This method should build the desired topology by creating Processors and Streams and connecting them to each other.

#####2.2 `Topology getTopology()`
This method should return the topology built by `init` to the engine for execution.

#####2.3 `void setFactory(ComponentFactory factory)`
Utility method to accept a `ComponentFactory` to use in building the topology.

