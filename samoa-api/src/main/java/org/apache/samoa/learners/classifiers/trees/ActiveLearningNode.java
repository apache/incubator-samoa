package org.apache.samoa.learners.classifiers.trees;

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

import java.util.*;

import com.google.common.collect.EvictingQueue;
import org.apache.samoa.instances.Attribute;
import org.apache.samoa.learners.classifiers.ModelAggregator;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.learners.classifiers.ensemble.AttributeSliceEvent;
import org.apache.samoa.learners.classifiers.ensemble.BoostMAProcessor;
import org.apache.samoa.moa.classifiers.core.AttributeSplitSuggestion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ActiveLearningNode extends LearningNode {
  /**
	 *
	 */
  public enum SplittingOption {THROW_AWAY, KEEP};

  private static final long serialVersionUID = -2892102872646338908L;
  private static final Logger logger = LoggerFactory.getLogger(ActiveLearningNode.class);

  private final SplittingOption splittingOption;
  private final int maxBufferSize;
  private final Queue<Instance> buffer;

  private double weightSeenAtLastSplitEvaluation;

  private final Map<Integer, String> attributeContentEventKeys;

  private AttributeSplitSuggestion bestSuggestion;
  private AttributeSplitSuggestion secondBestSuggestion;

  private final long id;
  private final int parallelismHint;
  private int suggestionCtr;
  private int thrownAwayInstance;

  private int ensembleId; //faye boostVHT

  private boolean isSplitting;

  public ActiveLearningNode(double[] classObservation, int parallelismHint, SplittingOption splitOption, int maxBufferSize) {
    super(classObservation);
    this.weightSeenAtLastSplitEvaluation = this.getWeightSeen();
    this.id = VerticalHoeffdingTree.LearningNodeIdGenerator.generate(); //todo (faye) :: ask if this could affect the singleton property.
    this.attributeContentEventKeys = new HashMap<>();
    this.isSplitting = false;
    this.parallelismHint = parallelismHint;
    this.splittingOption = splitOption;
    this.maxBufferSize = maxBufferSize;
    this.buffer = EvictingQueue.create(maxBufferSize); // new ArrayDeque<>(maxBufferSize)
  }

  public long getId() {
    return id;
  }

  public AttributeBatchContentEvent[] attributeBatchContentEvent;

  public AttributeBatchContentEvent[] getAttributeBatchContentEvent() {
    return this.attributeBatchContentEvent;
  }

  public void setAttributeBatchContentEvent(AttributeBatchContentEvent[] attributeBatchContentEvent) {
    this.attributeBatchContentEvent = attributeBatchContentEvent;
  }

  @Override
  public void learnFromInstance(Instance inst, ModelAggregator proc) {
    if (isSplitting) {
      switch (this.splittingOption) {
        case THROW_AWAY:
          //logger.trace("node {}: splitting is happening, throw away the instance", this.id); // throw all instance will splitting
          this.thrownAwayInstance++;
          return;
        case KEEP:
          //logger.trace("node {}: keep instance with max buffer size: {}, continue sending to local stats", this.id, this.maxBufferSize);
            //logger.trace("node {}: add to buffer", this.id);
            buffer.add(inst);
          break;
        default:
          logger.error("node {}: invalid splittingOption option: {}", this.id, this.splittingOption);
          break;
      }
    }

    // What we do is slice up the attributes array into parallelismHint (no. of local stats processors - LSP)
    // and send only one message per LSP which contains that slice of the attributes along with required information
    // to update the class observers.
    // Given that we are sending slices, there's probably some optimizations that can be made at the LSP level,
    // like being smarter about how we update the observers.
    this.observedClassDistribution.addToValue((int) inst.classValue(),
        inst.weight());
    double[] attributeArray =  inst.toDoubleArray();
    int sliceSize = (attributeArray.length - 1) / parallelismHint;
    boolean[] isNominalAll = new boolean[inst.numAttributes() - 1];
    for (int i = 0; i < inst.numAttributes() - 1; i++) {
      Attribute att = inst.attribute(i);
      if (att.isNominal()) {
        isNominalAll[i] = true;
      }
    }
    int startingIndex = 0;
    for (int localStatsIndex = 0; localStatsIndex < parallelismHint; localStatsIndex++) {
      // The endpoint for the slice is either the end of the previous slice, or the end of the array
      // TODO: Note that we assume class is at the end of the instance attribute array, hence the length-1 here
      // We can do proper handling later
      int endpoint = localStatsIndex == (parallelismHint - 1) ? (attributeArray.length-1) : (localStatsIndex + 1) * sliceSize;
      double[] attributeSlice = Arrays.copyOfRange(
          attributeArray, localStatsIndex * sliceSize, endpoint);
      boolean[] isNominalSlice = Arrays.copyOfRange(
          isNominalAll, localStatsIndex * sliceSize, endpoint);
      AttributeSliceEvent attributeSliceEvent = new AttributeSliceEvent(
          this.id, startingIndex, Integer.toString(localStatsIndex), isNominalSlice, attributeSlice,
          (int) inst.classValue(), inst.weight());
      proc.sendToAttributeStream(attributeSliceEvent);
      startingIndex = endpoint;
    }
  }

  @Override
  public double[] getClassVotes(Instance inst, ModelAggregator map) {
    return this.observedClassDistribution.getArrayCopy();
  }

  public double getWeightSeen() {
    return this.observedClassDistribution.sumOfValues();
  }

  public void setWeightSeenAtLastSplitEvaluation(double weight) {
    this.weightSeenAtLastSplitEvaluation = weight;
  }

  public double getWeightSeenAtLastSplitEvaluation() {
    return this.weightSeenAtLastSplitEvaluation;
  }

  public void requestDistributedSuggestions(long splitId, ModelAggregator modelAggrProc) {
    this.isSplitting = true;
    this.suggestionCtr = 0;
    this.thrownAwayInstance = 0;

    // Possible this is causing unnecessary delay, can check if we can remove the abstraction to optimize
    ComputeContentEvent cce = new ComputeContentEvent(splitId, this.id,
        this.getObservedClassDistribution());
    cce.setEnsembleId(this.ensembleId);
    modelAggrProc.sendToControlStream(cce);
  }

  public void addDistributedSuggestions(AttributeSplitSuggestion bestSuggestion, AttributeSplitSuggestion secondBestSuggestion) {
    // starts comparing from the best suggestion
    if (bestSuggestion != null) {
      if ((this.bestSuggestion == null) || (bestSuggestion.compareTo(this.bestSuggestion) > 0)) {
        this.secondBestSuggestion = this.bestSuggestion;
        this.bestSuggestion = bestSuggestion;

        if (secondBestSuggestion != null) {

          if ((this.secondBestSuggestion == null) || (secondBestSuggestion.compareTo(this.secondBestSuggestion) > 0)) {
            this.secondBestSuggestion = secondBestSuggestion;
          }
        }
      } else {
        if ((this.secondBestSuggestion == null) || (bestSuggestion.compareTo(this.secondBestSuggestion) > 0)) {
          this.secondBestSuggestion = bestSuggestion;
        }
      }
    }

    // TODO: optimize the code to use less memory
    this.suggestionCtr++;
  }

  public boolean isSplitting() {
    return this.isSplitting;
  }

  public void endSplitting() {
    this.isSplitting = false;
//    logger.trace("wasted instance: {}", this.thrownAwayInstance);
//    logger.debug("node: {}. end splitting, thrown away instance: {}, buffer size: {}", this.id, this.thrownAwayInstance, this.buffer.size());
    this.thrownAwayInstance = 0;
    this.bestSuggestion = null;
    this.secondBestSuggestion = null;
    this.buffer.clear();
  }

  public AttributeSplitSuggestion getDistributedBestSuggestion() {
    return this.bestSuggestion;
  }

  public AttributeSplitSuggestion getDistributedSecondBestSuggestion() {
    return this.secondBestSuggestion;
  }

  public boolean isAllSuggestionsCollected() {
    return (this.suggestionCtr == this.parallelismHint);
  }

  private static int modelAttIndexToInstanceAttIndex(int index, Instance inst) {
    return inst.classIndex() > index ? index : index + 1;
  }

  private String generateKey(int obsIndex) {
    return Integer.toString(obsIndex % parallelismHint);
  }

  public Queue<Instance> getBuffer() {
    return buffer;
  }

  //-----------------faye boostVHT
  public int getEnsembleId() {
    return ensembleId;
  }
  
  public void setEnsembleId(int ensembleId) {
    this.ensembleId = ensembleId;
  }
}
