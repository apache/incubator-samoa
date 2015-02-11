---
title: Processing Item
layout: documentation
documentation: true
---
Processing Item is a hidden physical unit of the topology and is just a wrapper of Processor.
It is used internally, and it is not accessible from the API.

### Advanced 

It does not contain any logic but connects the Processor to the other processors in the topology.
There are two types of Processing Items.

1. Simple Processing Item (PI)
2. Entrance Processing Item (EntrancePI)

#### 1. Simple Processing Item (PI)
Once a Processor is wrapped in a PI, it becomes an executable component of the topology. All physical topology units are created with the help of a `TopologyBuilder`. Following code snippet shows the creation of a Processing Item.

```
builder.initTopology("MyTopology");
Processor samplerProcessor = new Sampler();
ProcessingItem samplerPI = builder.createPI(samplerProcessor,3);
```
The `createPI()` method of `TopologyBuilder` is used to create a PI. Its first argument is the instance of a Processor which needs to be wrapped-in. Its second argument is the parallelism hint. It tells the underlying platforms how many parallel instances of this PI should be created on different nodes.

#### 2. Entrance Processing Item (EntrancePI)
Entrance Processing Item is different from a PI in only one way: it accepts an Entrance Processor which can generate its own stream.
It is mostly used as the source of a topology.
It connects to external sources, pulls data and provides it to the topology in the form of streams.
All physical topology units are created with the help of a `TopologyBuilder`.
The following code snippet shows the creation of an Entrance Processing Item.

```
builder.initTopology("MyTopology");
EntranceProcessor sourceProcessor = new Source();
EntranceProcessingItem sourcePi = builder.createEntrancePi(sourceProcessor);
```
