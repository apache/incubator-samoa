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

import com.google.common.collect.EvictingQueue;
import org.apache.samoa.instances.Attribute;
import org.apache.samoa.instances.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Queue;

public final class BoostVHTActiveLearningNode extends ActiveLearningNode {

  public enum SplittingOption {
    THROW_AWAY, KEEP
  }

  private static final Logger logger = LoggerFactory.getLogger(BoostVHTActiveLearningNode.class);

  private final SplittingOption splittingOption;
  private final int maxBufferSize;
  private final Queue<Instance> buffer;
  private int ensembleId;

  public BoostVHTActiveLearningNode(double[] classObservation, int parallelism_hint, SplittingOption splitOption, int maxBufferSize) {
    super(classObservation, parallelism_hint);
    weightSeenAtLastSplitEvaluation = this.getWeightSeen();
    id = VerticalHoeffdingTree.LearningNodeIdGenerator.generate();
    attributeContentEventKeys = new HashMap<>();
    isSplitting = false;
    parallelismHint = parallelism_hint;
    this.splittingOption = splitOption;
    this.maxBufferSize = maxBufferSize;
    this.buffer = EvictingQueue.create(maxBufferSize);
  }

  @Override
  public void learnFromInstance(Instance inst, ModelAggregatorProcessor proc) {
    if (isSplitting) {
      switch (this.splittingOption) {
        case THROW_AWAY:
          //logger.trace("node {}: splitting is happening, throw away the instance", this.id); // throw all instance will splitting
          thrownAwayInstance++;
          return;
        case KEEP:
          //logger.trace("node {}: keep instance with max buffer size: {}, continue sending to local stats", this.id, this.maxBufferSize);
          //logger.trace("node {}: add to buffer", this.id);
          buffer.add(inst);
          break;
        default:
          logger.error("node {}: invalid splittingOption option: {}", id, this.splittingOption);
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
    double[] attributeArray = inst.toDoubleArray();
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
      int endpoint = localStatsIndex == (parallelismHint - 1) ? (attributeArray.length - 1) : (localStatsIndex + 1) * sliceSize;
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
  public void requestDistributedSuggestions(long splitId, ModelAggregatorProcessor modelAggrProc) {
    this.isSplitting = true;
    this.suggestionCtr = 0;
    this.thrownAwayInstance = 0;

    ComputeContentEvent cce = new ComputeContentEvent(splitId, this.id,
            this.getObservedClassDistribution());
    cce.setEnsembleId(this.ensembleId);
    modelAggrProc.sendToControlStream(cce);
  }

  @Override
  public void endSplitting() {
    super.endSplitting();
    this.buffer.clear();
  }

  @Override
  protected String generateKey(int obsIndex) {
    return Integer.toString(obsIndex % parallelismHint);
  }

  public Queue<Instance> getBuffer() {
    return buffer;
  }

  public int getEnsembleId() {
    return ensembleId;
  }

  public void setEnsembleId(int ensembleId) {
    this.ensembleId = ensembleId;
  }
}
