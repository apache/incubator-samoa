---
title: Getting Started
layout: documentation
documentation: true
---
We start showing how simple is to run a first large scale machine learning task in SAMOA. We will evaluate a bagging ensemble method using decision trees on the Forest Covertype dataset.

* 1. Download SAMOA 

```bash
git clone http://git.apache.org/incubator-samoa.git
cd incubator-samoa
mvn package      #Local mode
```
* 2. Download the Forest CoverType dataset 

```bash
wget "http://downloads.sourceforge.net/project/moa-datastream/Datasets/Classification/covtypeNorm.arff.zip"
unzip covtypeNorm.arff.zip 
```

_Forest Covertype_ contains the forest cover type for 30 x 30 meter cells obtained from the US Forest Service (USFS) Region 2 Resource Information System (RIS) data. It contains 581,012 instances and 54 attributes, and it has been used in several articles on data stream classification.

* 3.  Run an example: classifying the CoverType dataset with the bagging algorithm

```bash
bin/samoa local target/SAMOA-Local-0.3.0-SNAPSHOT.jar "PrequentialEvaluation -l classifiers.ensemble.Bagging 
    -s (ArffFileStream -f covtypeNorm.arff) -f 100000"
```


The output will be a list of the evaluation results, plotted each 100,000 instances.
