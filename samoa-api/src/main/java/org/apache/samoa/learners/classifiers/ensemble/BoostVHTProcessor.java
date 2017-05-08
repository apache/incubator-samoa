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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.learners.classifiers.trees.BoostVHTActiveLearningNode.SplittingOption;
import org.apache.samoa.learners.classifiers.trees.LocalResultContentEvent;
import org.apache.samoa.moa.classifiers.core.splitcriteria.InfoGainSplitCriterion;
import org.apache.samoa.moa.classifiers.core.splitcriteria.SplitCriterion;
import org.apache.samoa.moa.core.DoubleVector;
import org.apache.samoa.moa.core.MiscUtils;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * The Class BoostVHTProcessor.
 */
public class BoostVHTProcessor implements Processor {

  private static final long serialVersionUID = -1550901409625192730L;
  private static final Logger logger = LoggerFactory.getLogger(BoostVHTProcessor.class);
  
  //The following are configured from the user in BoostVHT
  private SplitCriterion splitCriterion;
  
  private Double splitConfidence;
  
  private Double tieThreshold;
  
  private int gracePeriod;
  
  private int parallelismHint;
  
  private int timeOut;

  private SplittingOption splittingOption;

  /** The input dataset to BoostVHT. */
  private Instances dataset;
  
  /** The ensemble size. */
  private int ensembleSize;
  
  /** The result stream. */
  private Stream resultStream;
  
  /** The control stream. */
  private Stream controlStream;
  
  /** The attribute stream. */
  private Stream attributeStream;
  
  protected BoostMAProcessor[] mAPEnsemble;

  /** Ramdom number generator. */
  protected Random random;

  private int seed;

  // lambda_m correct
  protected double[] scms;
  // lambda_m wrong
  protected double[] swms;
  private double[] e_m;

  private double trainingWeightSeenByModel;

  private int numberOfClasses;

  private int maxBufferSize;

  private BoostVHTProcessor(Builder builder) {
    this.dataset = builder.dataset;
    this.ensembleSize = builder.ensembleSize;
    this.seed = builder.seed;
    this.numberOfClasses = builder.numberOfClasses;
    this.splitCriterion = builder.splitCriterion;
    this.splitConfidence = builder.splitConfidence;
    this.tieThreshold = builder.tieThreshold;
    this.gracePeriod = builder.gracePeriod;
    this.parallelismHint = builder.parallelismHint;
    this.timeOut = builder.timeOut;
    this.splittingOption = builder.splittingOption;
    this.maxBufferSize = builder.maxBufferSize;
  }

  /**
   * On event.
   * 
   * @param event the event
   * @return true, if successful
   */
  public boolean process(ContentEvent event) {

    if (event instanceof InstanceContentEvent) {
      InstanceContentEvent inEvent = (InstanceContentEvent) event;
      //todo:: check if any precondition is needed

      if (inEvent.isTesting()) {
        double[] combinedPrediction = computeBoosting(inEvent);
        this.resultStream.put(newResultContentEvent(combinedPrediction, inEvent));
      }

      // estimate model parameters using the training data
      if (inEvent.isTraining()) {
        train(inEvent);
      }
    } else if (event instanceof LocalResultContentEvent) {
      LocalResultContentEvent lrce = (LocalResultContentEvent) event;
      mAPEnsemble[lrce.getEnsembleId()].updateModel(lrce);
    }


    return true;
  }

  @Override
  public void onCreate(int id) {
    
    mAPEnsemble = new BoostMAProcessor[ensembleSize];

    random = new Random(seed);

    this.scms = new double[ensembleSize];
    this.swms = new double[ensembleSize];
    this.e_m = new double[ensembleSize];

    //instantiate the MAs
    for (int i = 0; i < ensembleSize; i++) {
      BoostMAProcessor newProc = new BoostMAProcessor.BoostMABuilder(dataset)
          .splitCriterion(splitCriterion)
          .splitConfidence(splitConfidence)
          .tieThreshold(tieThreshold)
          .gracePeriod(gracePeriod)
          .parallelismHint(parallelismHint)
          .timeOut(timeOut)
          .processorID(i) // The BoostMA processors get incremental ids
          .maxBufferSize(maxBufferSize)
          .splittingOption(splittingOption)
          .build();
      newProc.setAttributeStream(this.attributeStream);
      newProc.setControlStream(this.controlStream);
      mAPEnsemble[i] = newProc;
    }
  }
  
  private double[] computeBoosting(InstanceContentEvent inEvent) {
    
    Instance testInstance = inEvent.getInstance();
    DoubleVector combinedPredictions = new DoubleVector();

    for (int i = 0; i < ensembleSize; i++) {
      double memberWeight = getEnsembleMemberWeight(i);
      if (memberWeight > 0.0) {
        DoubleVector vote = new DoubleVector(mAPEnsemble[i].getVotesForInstance(testInstance));
        if (vote.sumOfValues() > 0.0) {
          vote.normalize();
          vote.scaleValues(memberWeight);
          combinedPredictions.addValues(vote);
        }
      } else {
        break;
      }
    }
    return combinedPredictions.getArrayRef();
  }
  
  /**
   * Train.
   *
   * @param inEvent
   *          the in event
   */
  protected void train(InstanceContentEvent inEvent) {
    Instance trainInstance = inEvent.getInstance();

    this.trainingWeightSeenByModel += trainInstance.weight();
    double lambda_d = 1.0;
    
    for (int i = 0; i < ensembleSize; i++) { //for each base model
      int k = MiscUtils.poisson(lambda_d, this.random); //set k according to poisson

      if (k > 0) {
        Instance weightedInstance = trainInstance.copy();
        weightedInstance.setWeight(trainInstance.weight() * k);
        mAPEnsemble[i].trainOnInstanceImpl(weightedInstance);
      }
      //get prediction for the instance from the specific learner of the ensemble
      double[] prediction = mAPEnsemble[i].getVotesForInstance(trainInstance);

      if (mAPEnsemble[i].correctlyClassifies(trainInstance,prediction)) {
        this.scms[i] += lambda_d;
        lambda_d *= this.trainingWeightSeenByModel / (2 * this.scms[i]);
      } else {
        this.swms[i] += lambda_d;
        lambda_d *= this.trainingWeightSeenByModel / (2 * this.swms[i]);
      }
    }
  }
  
  private double getEnsembleMemberWeight(int i) {
    double em = this.swms[i] / (this.scms[i] + this.swms[i]);
//    if ((em == 0.0) || (em > 0.5)) {
    if ((em == 0.0) || (em > (1.0 - 1.0/this.numberOfClasses))) { //for SAMME
      return 0.0;
    }
    double Bm = em / (1.0 - em);
//    return Math.log(1.0 / Bm);
    return Math.log(1.0 / Bm ) + Math.log(this.numberOfClasses - 1); //for SAMME
  }
  
  /**
   * Helper method to generate new ResultContentEvent based on an instance and its prediction result.
   *
   * @param combinedPrediction
   *          The predicted class label from the Boost-VHT decision tree model.
   * @param inEvent
   *          The associated instance content event
   * @return ResultContentEvent to be sent into Evaluator PI or other destination PI.
   */
  private ResultContentEvent newResultContentEvent(double[] combinedPrediction, InstanceContentEvent inEvent) {
    ResultContentEvent rce = new ResultContentEvent(inEvent.getInstanceIndex(), inEvent.getInstance(),
            inEvent.getClassId(), combinedPrediction, inEvent.isLastEvent());
    rce.setEvaluationIndex(inEvent.getEvaluationIndex());
    return rce;
  }

  public static class Builder {
    // BoostVHT processor parameters
    private final Instances dataset;
    private int ensembleSize;
    private int numberOfClasses;

    // BoostMAProcessor parameters
    private SplitCriterion splitCriterion = new InfoGainSplitCriterion();
    private double splitConfidence;
    private double tieThreshold;
    private int gracePeriod;
    private int parallelismHint;
    private int timeOut = Integer.MAX_VALUE;
    private SplittingOption splittingOption;
    private int maxBufferSize;
    private int seed;

    public Builder(Instances dataset) {
      this.dataset = dataset;
    }

    public Builder(BoostVHTProcessor oldProcessor) {
      this.dataset = oldProcessor.getDataset();
      this.ensembleSize = oldProcessor.getEnsembleSize();
      this.numberOfClasses = oldProcessor.getNumberOfClasses();
      this.splitCriterion = oldProcessor.getSplitCriterion();
      this.splitConfidence = oldProcessor.getSplitConfidence();
      this.tieThreshold = oldProcessor.getTieThreshold();
      this.gracePeriod = oldProcessor.getGracePeriod();
      this.parallelismHint = oldProcessor.getParallelismHint();
      this.timeOut = oldProcessor.getTimeOut();
      this.splittingOption = oldProcessor.splittingOption;
      this.seed = oldProcessor.getSeed();
    }

    public Builder ensembleSize(int ensembleSize) {
      this.ensembleSize = ensembleSize;
      return this;
    }

    public Builder numberOfClasses(int numberOfClasses) {
      this.numberOfClasses = numberOfClasses;
      return this;
    }

    public Builder splitCriterion(SplitCriterion splitCriterion) {
      this.splitCriterion = splitCriterion;
      return this;
    }

    public Builder splitConfidence(double splitConfidence) {
      this.splitConfidence = splitConfidence;
      return this;
    }

    public Builder tieThreshold(double tieThreshold) {
      this.tieThreshold = tieThreshold;
      return this;
    }

    public Builder gracePeriod(int gracePeriod) {
      this.gracePeriod = gracePeriod;
      return this;
    }

    public Builder parallelismHint(int parallelismHint) {
      this.parallelismHint = parallelismHint;
      return this;
    }

    public Builder timeOut(int timeOut) {
      this.timeOut = timeOut;
      return this;
    }

    public Builder splittingOption(SplittingOption splittingOption) {
      this.splittingOption = splittingOption;
      return this;
    }

    public Builder maxBufferSize(int maxBufferSize) {
      this.maxBufferSize= maxBufferSize;
      return this;
    }

    public Builder seed(int seed) {
      this.seed = seed;
      return this;
    }

    public BoostVHTProcessor build() {
      return new BoostVHTProcessor(this);
    }
  }
  
  public Instances getInputInstances() {
    return dataset;
  }
  
  public void setInputInstances(Instances dataset) {
    this.dataset = dataset;
  }
  
  public Stream getResultStream() {
    return this.resultStream;
  }
  
  public void setResultStream(Stream resultStream) {
    this.resultStream = resultStream;
  }

  public int getEnsembleSize() {
    return ensembleSize;
  }

  public Stream getControlStream() {
    return controlStream;
  }
  
  public void setControlStream(Stream controlStream) {
    this.controlStream = controlStream;
  }

  public Stream getAttributeStream() {
    return attributeStream;
  }

  public void setAttributeStream(Stream attributeStream) {
    this.attributeStream = attributeStream;
  }

  public SplitCriterion getSplitCriterion() {
    return splitCriterion;
  }
  

  public Double getSplitConfidence() {
    return splitConfidence;
  }
  

  public Double getTieThreshold() {
    return tieThreshold;
  }

  public int getSeed() {
    return seed;
  }

  public int getGracePeriod() {
    return gracePeriod;
  }
  

  public int getParallelismHint() {
    return parallelismHint;
  }

  public int getTimeOut() {
    return timeOut;
  }
  
  public void setTimeOut(int timeOut) {
    this.timeOut = timeOut;
  }
  
  public int getNumberOfClasses() {
    return numberOfClasses;
  }
  
  public void setNumberOfClasses(int numberOfClasses) {
    this.numberOfClasses = numberOfClasses;
  }
  
  public Instances getDataset() {
    return dataset;
  }
  
  @Override
  public Processor newProcessor(Processor sourceProcessor) {
    BoostVHTProcessor originProcessor = (BoostVHTProcessor) sourceProcessor;
    BoostVHTProcessor newProcessor = new BoostVHTProcessor.Builder(originProcessor).build();

    if (originProcessor.getResultStream() != null) {
      newProcessor.setResultStream(originProcessor.getResultStream());
      newProcessor.setControlStream(originProcessor.getControlStream());
      newProcessor.setAttributeStream(originProcessor.getAttributeStream());
    }
    return newProcessor;
  }
}
