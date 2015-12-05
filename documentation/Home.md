---
title: Apache SAMOA Documentation
layout: documentation
documentation: true
---
Apache SAMOA is a platform for mining big data streams.

It provides a collection of distributed streaming algorithms for the most common data mining and machine learning tasks such as classification, clustering, and regression, as well as programming abstractions to develop new algorithms that run on top of distributed stream processing engines (DSPEs). It features a pluggable architecture that allows it to run on several DSPEs such as Apache Storm, Apache S4, Apache Samza, and Apache Flink.
SAMOA is similar to Mahout in spirit, but specific designed for stream mining.

Apache SAMOA is simple and fun to use! This documentation is intended to give an introduction on how to use SAMOA in different ways. As a user you can run SAMOA algorithms on several stream processing engines: local mode, Storm, S4, Samza, and Flink. As a developer you can create new algorithms only once and test them in all of these distributed stream processing engines.

## Getting Started
* [0 Hands-on with SAMOA: Getting Started!](Getting-Started.html)

## Users
* [1 Building and Executing SAMOA](Scalable-Advanced-Massive-Online-Analysis.html)
    * [1.0 Building SAMOA](Building-SAMOA.html)
    * [1.1 Executing SAMOA with Apache Storm](Executing-SAMOA-with-Apache-Storm.html)
    * [1.2 Executing SAMOA with Apache S4](Executing-SAMOA-with-Apache-S4.html)
    * [1.3 Executing SAMOA with Apache Samza](Executing-SAMOA-with-Apache-Samza.html)
	* [1.4 Executing SAMOA with Apache Avro Files](Executing-SAMOA-with-Apache-Avro-Files.html)
* [2 Machine Learning Methods in SAMOA](SAMOA-and-Machine-Learning.html)
    * [2.1 Prequential Evaluation Task](Prequential-Evaluation-Task.html)
    * [2.2 Vertical Hoeffding Tree Classifier](Vertical-Hoeffding-Tree-Classifier.html)
    * [2.3 Adaptive Model Rules Regressor](Adaptive-Model-Rules-Regressor.html)
    * [2.4 Bagging and Boosting](Bagging-and-Boosting.html)
    * [2.5 Distributed Stream Clustering](Distributed-Stream-Clustering.html)
    * [2.6 Distributed Stream Frequent Itemset Mining](Distributed-Stream-Frequent-Itemset-Mining.html)
    * [2.7 SAMOA for MOA users](SAMOA-for-MOA-users.html)

## Developers
* [3 Understanding SAMOA Topologies](SAMOA-Topology.html)
    * [3.1 Processor](Processor.html)
    * [3.2 Content Event](Content-Event.html)
    * [3.3 Stream](Stream.html)
    * [3.4 Task](Task.html)
    * [3.5 Topology Builder](Topology-Builder.html)
    * [3.6 Learner](Learner.html)
    * [3.7 Processing Item](Processing-Item.html)
* [4 Developing New Tasks in SAMOA](Developing-New-Tasks-in-SAMOA.html)

### Getting help
Discussion about SAMOA happens on the Apache development mailing list [dev@samoa.incubator.org](mailto:dev@samoa.incubator.org)

[ [subscribe](mailto:dev-subscribe@samoa.incubator.org) | [unsubscribe](mailto:dev-unsubscribe@samoa.incubator.org) | [archives](http://mail-archives.apache.org/mod_mbox/incubator-samoa-dev) ]
