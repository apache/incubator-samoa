---
title: Topology Builder
layout: documentation
documentation: true
---
`TopologyBuilder` is a builder class which builds physical units of the topology and assemble them together. Each topology has a name. Following code snippet shows all the steps of creating a topology with one `EntrancePI`, two PIs and a few streams.

```
TopologyBuilder builder = new TopologyBuilder(factory) // ComponentFactory factory
builder.initTopology("Parma Topology"); //initiates an empty topology with a name
//********************************Topology building***********************************
StreamSource sourceProcessor = new StreamSource(inputPath,d,sampleSize,fpmGap,epsilon,phi,numSamples);
builder.addEntranceProcessor(sourceProcessor);
Stream sourceDataStream = builder.createStream(sourceProcessor);
sourceProcessor.setDataStream(sourceDataStream);
Stream sourceControlStream = builder.createStream(sourceProcessor);
sourceProcessor.setControlStream(sourceControlStream);

Sampler sampler = new Sampler(minFreqPercent,sampleSize,(float)epsilon,outputPath,sampler);
builder.addProcessor(sampler, numSamples);
builder.connectInputAllStream(sourceControlStream, sampler);
builder.connectInputShuffleStream(sourceDataStream, sampler);

Stream samplerDataStream = builder.createStream(sampler);
samplerP.setSamplerDataStream(samplerDataStream);
Stream samplerControlStream = builder.createStream(sampler);
samplerP.setSamplerControlStream(samplerControlStream);

Aggregator aggregatorProcessor = new Aggregator(outputPath,(long)numSamples,(long)sampleSize,(long)reqApproxNum,(float)epsilon);
builder.addProcessor(aggregatorProcessor, numAggregators);
builder.connectInputKeyStream(samplerDataStream, aggregatorProcessor);
builder.connectInputAllStream(samplerControlStream, aggregatorProcessor);
```
