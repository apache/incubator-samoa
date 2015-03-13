---
title: Learner
layout: documentation
documentation: true
---
Learners are implemented in SAMOA as sub-topologies.

```
public interface Learner extends Serializable{
	
	public void init(TopologyBuilder topologyBuilder, Instances dataset);

	public Processor getInputProcessor();

	public Stream getResultStream();
}
```
When a `Task` object is initiated via `init()`, the method `init(...)` of `Learner` is called, and the topology is added to the global topology of the task.

To create a new learner, it is only needed to add streams, processors and their connections to the topology in `init(...)`, specify what is the processor that will manage the input stream of the learner in `getInputProcessor()`, and finally, specify what is going to be the output stream of the learner with `getResultStream()`.
