/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samoa.learners.classifiers.ensemble;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.classifiers.trees.*;
import org.apache.samoa.learners.classifiers.trees.BoostVHTActiveLearningNode.SplittingOption;
import org.apache.samoa.moa.classifiers.core.AttributeSplitSuggestion;
import org.apache.samoa.moa.classifiers.core.splitcriteria.InfoGainSplitCriterion;
import org.apache.samoa.moa.classifiers.core.splitcriteria.SplitCriterion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * Boost Model Aggegator Processor consists of the decision tree model. It connects to local-statistic PI via attribute stream
 * and control stream. Model-aggregator PI sends the split instances via attribute stream and it sends control messages
 * to ask local-statistic PI to perform computation via control stream.
 * <p>
 * The calculation results from local statistic arrive to the model-aggregator PI via computation-result
 * stream.
 *
 * @author Arinto Murdopo
 */
public final class BoostMAProcessor extends ModelAggregatorProcessor {

  private static final Logger logger = LoggerFactory.getLogger(BoostMAProcessor.class);

  private final SplittingOption splittingOption;
  private final int maxBufferSize;

  // private constructor based on Builder pattern
  private BoostMAProcessor(BoostMABuilder builder) {
    super(builder);
    this.splittingOption = builder.splittingOption;
    this.maxBufferSize = builder.maxBufferSize;

    // These used to happen in onCreate which no longer gets called.
    activeLeafNodeCount = 0;
    inactiveLeafNodeCount = 0;
    decisionNodeCount = 0;
    growthAllowed = true;

    splittingNodes = new ConcurrentHashMap<>();
    splitId = 0;

    // Executor for scheduling time-out threads
    executor = Executors.newScheduledThreadPool(8);
  }

  public void updateModel(LocalResultContentEvent lrce) {

    Long lrceSplitId = lrce.getSplitId();
    SplittingNodeInfo splittingNodeInfo = splittingNodes.get(lrceSplitId);

    if (splittingNodeInfo != null) {
      BoostVHTActiveLearningNode bVHTactiveLearningNode = (BoostVHTActiveLearningNode)splittingNodeInfo.getActiveLearningNode();

      bVHTactiveLearningNode.addDistributedSuggestions(lrce.getBestSuggestion(), lrce.getSecondBestSuggestion());

      if (bVHTactiveLearningNode.isAllSuggestionsCollected()) {
        this.splittingNodes.remove(lrceSplitId);
        this.continueAttemptToSplit(bVHTactiveLearningNode, splittingNodeInfo.getFoundNode());
      }
    }
  }

  @Override
  public boolean process(ContentEvent event) {
    throw new NotImplementedException();
  }

  @Override
  public void trainOnInstanceImpl(FoundNode foundNode, Instance inst) {

    Node leafNode = foundNode.getNode();

    if (leafNode == null) {
      leafNode = newLearningNode(parallelismHint);
      foundNode.getParent().setChild(foundNode.getParentBranch(), leafNode);
      activeLeafNodeCount++;
    }

    if (leafNode instanceof LearningNode) {
      LearningNode learningNode = (LearningNode) leafNode;
      learningNode.learnFromInstance(inst, this);
    }

    if (leafNode instanceof BoostVHTActiveLearningNode) {
      BoostVHTActiveLearningNode activeLearningNode = (BoostVHTActiveLearningNode) leafNode;
      // See if we can ask for splits
      if (!activeLearningNode.isSplitting()) {
        double weightSeen = activeLearningNode.getWeightSeen();
        // check whether it is the time for splitting
        if (weightSeen - activeLearningNode.getWeightSeenAtLastSplitEvaluation() >= gracePeriod) {
          attemptToSplit(activeLearningNode, foundNode);
        }
      }
    }

  }

  @Override
  public void onCreate(int id) {
    this.resetLearning();
  }

  @Override
  public Processor newProcessor(Processor p) {
    BoostMAProcessor oldProcessor = (BoostMAProcessor) p;
    BoostMAProcessor newProcessor = new BoostMAProcessor.BoostMABuilder(oldProcessor).build();

    newProcessor.setAttributeStream(oldProcessor.getAttributeStream());
    newProcessor.setControlStream(oldProcessor.getControlStream());

    return newProcessor;
  }

  /**
   * Helper method to represent a split attempt
   *
   * @param bVHTactiveLearningNode The corresponding boostVHT active learning node which will be split
   * @param foundNode          The data structure to represents the filtering of the instance using the tree model.
   */
  @Override
  public void attemptToSplit(ActiveLearningNode bVHTactiveLearningNode, FoundNode foundNode) {
    if (!bVHTactiveLearningNode.observedClassDistributionIsPure()) {
      // Increment the split ID
      this.splitId++;

      this.splittingNodes.put(this.splitId, new SplittingNodeInfo((BoostVHTActiveLearningNode)bVHTactiveLearningNode, foundNode));

      // Inform Local Statistic PI to perform local statistic calculation
      bVHTactiveLearningNode.requestDistributedSuggestions(this.splitId, this);
    }
  }

  /**
   * Helper method to continue the attempt to split once all local calculation results are received.
   *
   * @param bvhtActiveLearningNode The corresponding active learning node which will be split
   * @param foundNode          The data structure to represents the filtering of the instance using the tree model.
   */
  @Override
  public void continueAttemptToSplit(ActiveLearningNode bvhtActiveLearningNode, FoundNode foundNode) {
    BoostVHTActiveLearningNode bVHTActiveLearningNode = (BoostVHTActiveLearningNode)bvhtActiveLearningNode;
    AttributeSplitSuggestion bestSuggestion = bVHTActiveLearningNode.getDistributedBestSuggestion();
    AttributeSplitSuggestion secondBestSuggestion = bVHTActiveLearningNode.getDistributedSecondBestSuggestion();

    // compare with null split
    double[] preSplitDist = bVHTActiveLearningNode.getObservedClassDistribution();
    AttributeSplitSuggestion nullSplit = new AttributeSplitSuggestion(null, new double[0][],
        this.splitCriterion.getMeritOfSplit(preSplitDist, new double[][] { preSplitDist }));

    if ((bestSuggestion == null) || (nullSplit.compareTo(bestSuggestion) > 0)) {
      secondBestSuggestion = bestSuggestion;
      bestSuggestion = nullSplit;
    } else {
      if ((secondBestSuggestion == null) || (nullSplit.compareTo(secondBestSuggestion) > 0)) {
        secondBestSuggestion = nullSplit;
      }
    }

    boolean shouldSplit = false;

    if (secondBestSuggestion == null) {
      shouldSplit = true;
    } else {
      double hoeffdingBound = computeHoeffdingBound(
          this.splitCriterion.getRangeOfMerit(bVHTActiveLearningNode.getObservedClassDistribution()), this.splitConfidence,
              bVHTActiveLearningNode.getWeightSeen());

      if ((bestSuggestion.merit - secondBestSuggestion.merit > hoeffdingBound) || (hoeffdingBound < tieThreshold)) {
        shouldSplit = true;
      }
      // TODO: add poor attributes removal
    }

    SplitNode parent = foundNode.getParent();
    int parentBranch = foundNode.getParentBranch();

    // split if the Hoeffding bound condition is satisfied
    if (shouldSplit) {

      if (bestSuggestion.splitTest != null) { // TODO: What happens when bestSuggestion is null? -> Deactivate node?
        SplitNode newSplit = new SplitNode(bestSuggestion.splitTest, bVHTActiveLearningNode.getObservedClassDistribution());

        for (int i = 0; i < bestSuggestion.numSplits(); i++) {
          Node newChild = newLearningNode(bestSuggestion.resultingClassDistributionFromSplit(i), this.parallelismHint);
          newSplit.setChild(i, newChild);
        }

        this.activeLeafNodeCount--;
        this.decisionNodeCount++;
        this.activeLeafNodeCount += bestSuggestion.numSplits();

        if (parent == null) {
          this.treeRoot = newSplit;
        } else {
          parent.setChild(parentBranch, newSplit);
        }
        //if keep w buffer
        if (splittingOption == SplittingOption.KEEP && this.maxBufferSize > 0) {
          Queue<Instance> buffer = bVHTActiveLearningNode.getBuffer();
//          logger.debug("node: {}. split is happening, there are {} items in buffer", activeLearningNode.getId(), buffer.size());
          while (!buffer.isEmpty()) {
            this.trainOnInstanceImpl(buffer.poll());
          }
        }
      }
      // TODO: add check on the model's memory size
    }

    // housekeeping
    bVHTActiveLearningNode.endSplitting();
    bVHTActiveLearningNode.setWeightSeenAtLastSplitEvaluation(bVHTActiveLearningNode.getWeightSeen());
  }

  @Override
  protected LearningNode newLearningNode(double[] initialClassObservations, int parallelismHint) {
    BoostVHTActiveLearningNode newNode = new BoostVHTActiveLearningNode(initialClassObservations, parallelismHint,
            this.splittingOption, this.maxBufferSize);
    newNode.setEnsembleId(this.processorId);
    return newNode;
  }

  /**
   * Builder class to replace constructors with many parameters
   *
   * @author Arinto Murdopo
   */
  public static class BoostMABuilder extends ModelAggregatorProcessor.Builder<BoostMABuilder> {

    // default values
    private SplitCriterion splitCriterion = new InfoGainSplitCriterion();
    private double splitConfidence = 0.0000001;
    private double tieThreshold = 0.05;
    private int gracePeriod = 200;
    private int parallelismHint = 1;
    private long timeOut = Integer.MAX_VALUE;
    private SplittingOption splittingOption;
    private int maxBufferSize = 0;

    public BoostMABuilder(Instances dataset) {
      super(dataset);
    }

    public BoostMABuilder(BoostMAProcessor oldProcessor) {
      super(oldProcessor);
      this.splittingOption = oldProcessor.splittingOption;
      this.maxBufferSize = oldProcessor.maxBufferSize;
    }

    public BoostMABuilder splittingOption(SplittingOption splittingOption) {
      this.splittingOption = splittingOption;
      return this;
    }

    public BoostMABuilder maxBufferSize(int maxBufferSize) {
      this.maxBufferSize = maxBufferSize;
      return this;
    }

    public BoostMAProcessor build() {
      return new BoostMAProcessor(this);
    }

  }
}
