package org.apache.samoa.learners.classifiers.ensemble;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

/**
 * License
 */

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import com.google.common.collect.ImmutableSet;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.ClassificationLearner;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.classifiers.trees.LocalStatisticsProcessor;
import org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.AttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.DiscreteAttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.NumericAttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.splitcriteria.SplitCriterion;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * The Bagging Classifier by Oza and Russell.
 */
public class BoostVHT implements ClassificationLearner, Configurable {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = -7523211543185584536L;
  
  private static final Logger logger =
          LoggerFactory.getLogger(BoostVHT.class);
  
  /** The base learner option. */ //TODO(tvas): Not being used currently, adding it to ease the automated Python scripts
  public ClassOption baseLearnerOption = new ClassOption("baseLearner", 'l',
      "Classifier to train.", Learner.class, VerticalHoeffdingTree.class.getName());
  
  public ClassOption numericEstimatorOption = new ClassOption("numericEstimator",
          'n', "Numeric estimator to use.", NumericAttributeClassObserver.class,
          "GaussianNumericAttributeClassObserver");

  public ClassOption nominalEstimatorOption = new ClassOption("nominalEstimator",
          'd', "Nominal estimator to use.", DiscreteAttributeClassObserver.class,
          "NominalAttributeClassObserver");

  public ClassOption splitCriterionOption = new ClassOption("splitCriterion",
          'r', "Split criterion to use.", SplitCriterion.class,
          "InfoGainSplitCriterion");

  public FloatOption splitConfidenceOption = new FloatOption("splitConfidence", 'c',
          "The allowable error in split decision, values closer to 0 will take longer to decide.",
          0.0000001, 0.0, 1.0);

  public FloatOption tieThresholdOption = new FloatOption("tieThreshold",
          't', "Threshold below which a split will be forced to break ties.",
          0.05, 0.0, 1.0);

  public IntOption gracePeriodOption = new IntOption("gracePeriod", 'g',
          "The number of instances a leaf should observe between split attempts.",
          200, 0, Integer.MAX_VALUE);

  public IntOption timeOutOption = new IntOption("timeOut", 'o',
          "The duration to wait all distributed computation results from local statistics PI",
          Integer.MAX_VALUE, 1, Integer.MAX_VALUE);

  public FlagOption binarySplitsOption = new FlagOption("binarySplits", 'b',
          "Only allow binary splits.");
  
    /** The ensemble size option. */
  public IntOption ensembleSizeOption = new IntOption("ensembleSize", 's',
      "The number of models in the bag.", 10, 1, Integer.MAX_VALUE);

  /** The Model Aggregator boosting processor. */
  private BoostVHTProcessor boostVHTProcessor;
  
  /** The Local statistics processor. */
  private LocalStatisticsProcessor locStatProcessor;
  
  /** The result stream. */
  protected Stream resultStream;
  
  /** The attribute stream. */
  protected Stream attributeStream;
  
  /** The control stream. */
  protected Stream controlStream;
  
  /** The compute stream. */
  protected Stream computeStream;
  
  /** The dataset. */
  private Instances dataset;

  protected int parallelism;
  
  //for SAMMME
  public IntOption numberOfClassesOption = new IntOption("numberOfClasses", 'k',
          "The number of classes.", 2, 2, Integer.MAX_VALUE); //for SAMME
  
  //---

  /**
   * Sets the layout.
   */
  protected void setLayout() {

    int ensembleSize = this.ensembleSizeOption.getValue();

    // Set parameters for BoostVHT processor, and the BoostMA processors within.
    try {
      boostVHTProcessor = new BoostVHTProcessor.Builder(dataset)
          .ensembleSize(this.ensembleSizeOption.getValue())
          .numberOfClasses(this.numberOfClassesOption.getValue())
          .splitCriterion(
              (SplitCriterion) ClassOption.createObject(this.splitCriterionOption.getValueAsCLIString(),
              this.splitCriterionOption.getRequiredType()))
          .splitConfidence(this.splitConfidenceOption.getValue())
          .tieThreshold(this.tieThresholdOption.getValue())
          .gracePeriod(this.gracePeriodOption.getValue())
          .parallelismHint(this.ensembleSizeOption.getValue())
          .timeOut(this.timeOutOption.getValue())
          .build();
    } catch (Exception e) {
      e.printStackTrace();
    }

    //add Boosting Model Aggregator Processor to the topology
    this.topologyBuilder.addProcessor(boostVHTProcessor, 1);
    

    // Streams
    attributeStream = this.topologyBuilder.createStream(boostVHTProcessor);
    controlStream = this.topologyBuilder.createStream(boostVHTProcessor);
    
    //local statistics processor.
    locStatProcessor = new LocalStatisticsProcessor.Builder()
            .splitCriterion((SplitCriterion) this.splitCriterionOption.getValue())
            .binarySplit(this.binarySplitsOption.isSet())
            .nominalClassObserver((AttributeClassObserver) this.nominalEstimatorOption.getValue())
            .numericClassObserver((AttributeClassObserver) this.numericEstimatorOption.getValue())
            .build();
    
    this.topologyBuilder.addProcessor(locStatProcessor, ensembleSize);
  
    this.topologyBuilder.connectInputKeyStream(attributeStream, locStatProcessor);
    this.topologyBuilder.connectInputAllStream(controlStream, locStatProcessor);
    
    
    //local statistics result stream
    computeStream = this.topologyBuilder.createStream(locStatProcessor);
    locStatProcessor.setComputationResultStream(computeStream);
    this.topologyBuilder.connectInputAllStream(computeStream, boostVHTProcessor);
  
    //prediction is computed in boostVHTProcessor
    resultStream = this.topologyBuilder.createStream(boostVHTProcessor);
    
    //set the out streams of the BoostVHTProcessor
    boostVHTProcessor.setResultStream(resultStream);
    boostVHTProcessor.setAttributeStream(attributeStream);
    boostVHTProcessor.setControlStream(controlStream);
  }

  /** The topologyBuilder. */
  private TopologyBuilder topologyBuilder;

  /*
   * (non-Javadoc)
   * 
   * @see samoa.classifiers.Classifier#init(samoa.engines.Engine,
   * samoa.core.Stream, weka.core.Instances)
   */

  @Override
  public void init(TopologyBuilder builder, Instances dataset, int parallelism) {
    this.topologyBuilder = builder;
    this.dataset = dataset;
    this.parallelism = parallelism;
    this.setLayout();
  }

  @Override
  public Processor getInputProcessor() {
    return boostVHTProcessor;
  }

  /*
   * (non-Javadoc)
   * 
   * @see samoa.learners.Learner#getResultStreams()
   */
  @Override
  public Set<Stream> getResultStreams() {
    return ImmutableSet.of(this.resultStream);
  }
}
