---
title: Distributed Stream Clustering
layout: documentation
documentation: true
---
## Apache SAMOA Clustering Algorithm ##
The SAMOA Clustering Algorithm is invoked by using the `ClusteringEvaluation` task. The clustering task can be executed with default values just by running:

```
bin/samoa storm target/SAMOA-Storm-0.0.1-SNAPSHOT.jar "ClusteringEvaluation"
```

Parameters:

* `-l`: clusterer to train
* `-s`: stream to learn from
* `-i`: maximum number of instances to test/train on (-1 = no limit)
* `-f`: how many instances between samples of the learning performance
* `-n`: evaluation name (default: ClusteringEvaluation_TimeStamp)
* `-d`: file to append intermediate csv results to

In terms of the SAMOA API, Clustering Evaluation consists of a `source` processor, a `clusterer`, and a `evaluator` processor. `Source` processor sends the instances to the classifier using `source` stream. The clusterer sends the clustering results to the `evaluator` processor via the `result` stream. The `source Processor` corresponds to the `-s` option of Clustering Evaluation, and the clusterer corresponds to the `-l` option.
