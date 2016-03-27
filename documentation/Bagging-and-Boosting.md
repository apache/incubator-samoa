---
title: Bagging and Boosting
layout: documentation
documentation: true
---
Ensemble methods are combinations of several models whose individual predictions are combined in some manner (e.g., averaging or voting) to form a final prediction. When tackling non-stationary concepts, ensembles of classifiers have several advantages over single classifier methods: they are easy to scale and parallelize, they can adapt to change quickly by pruning under-performing parts of the ensemble, and they therefore usually also generate more accurate concept descriptions.

Bagging and boosting are traditional ensemble methods for streaming environments.
It is possible to use the classifiers available in [MOA](http://moa.cms.waikato.ac.nz) by using the [SAMOA-MOA](https://github.com/samoa-moa/samoa-moa) adapter.

### Bagging 
You can use Bagging as a SAMOA learner, specifying the number of learners to use with parameter `-s` and the base learner to use with parameter `-l`

`(classifiers.ensemble.Bagging -s 10 -l (classifiers.trees.VerticalHoeffdingTree))`

###### Only with SAMOA-MOA adapter
`(classifiers.ensemble.Bagging -s 10 -l (classifiers.SingleClassifier -l (MOAClassifierAdapter -l moa.classifiers.trees.HoeffdingTree)))`

### Adaptive Bagging
If data is evolving, it is better to use an adaptive version of bagging, where each base learner has a change detector that monitors its accuracy. When the accuracy of a base learner decreases, a new base learner is built to replace it.

`(classifiers.ensemble.AdaptiveBagging -s 10 -l (classifiers.trees.VerticalHoeffdingTree))`

###### Only with SAMOA-MOA adapter
`(classifiers.ensemble.AdaptiveBagging -s 10 -l (classifiers.SingleClassifier -l (org.apache.samoa.learners.classifiers.MOAClassifierAdapter -l moa.classifiers.trees.HoeffdingTree)))`

### Boosting
Boosting is a well known ensemble method, that has a very good performance in non-streaming setting. SAMOA implements the version of Oza and Russel (_Nikunj C. Oza, Stuart J. Russell: Experimental comparisons of online and batch versions of bagging and boosting. KDD 2001:359-364_)

`(classifiers.ensemble.Boosting -s 10 -l (classifiers.trees.VerticalHoeffdingTree))`

###### Only with SAMOA-MOA adapter
`(classifiers.ensemble.Boosting -s 10 -l (classifiers.SingleClassifier -l (MOAClassifierAdapter -l moa.classifiers.trees.HoeffdingTree)))`
