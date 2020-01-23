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

package org.apache.samoa.learners.classifiers.trees;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.moa.classifiers.core.AttributeSplitSuggestion;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.AttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.GaussianNumericAttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.NominalAttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.splitcriteria.InfoGainSplitCriterion;
import org.apache.samoa.moa.classifiers.core.splitcriteria.SplitCriterion;
import org.apache.samoa.topology.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

/**
 * Local Statistic Processor contains the local statistic of a subset of the attributes.
 * 
 * @author Arinto Murdopo
 * 
 */
public final class LocalStatisticsProcessor implements Processor {

  /**
	 * 
	 */
  private static final long serialVersionUID = -3967695130634517631L;
  private static Logger logger = LoggerFactory.getLogger(LocalStatisticsProcessor.class);

  // Collection of AttributeObservers, for each ActiveLearningNode and
  // AttributeId
  private Table<Long, Integer, AttributeClassObserver> localStats;

  private Stream computationResultStream;

  private final SplitCriterion splitCriterion;
  private final boolean binarySplit;
  private final AttributeClassObserver nominalClassObserver;
  private final AttributeClassObserver numericClassObserver;
  private int id;

  // the two observer classes below are also needed to be setup from the Tree
  private LocalStatisticsProcessor(Builder builder) {
    this.splitCriterion = builder.splitCriterion;
    this.binarySplit = builder.binarySplit;
    this.nominalClassObserver = builder.nominalClassObserver;
    this.numericClassObserver = builder.numericClassObserver;
  }

  @Override
  public boolean process(ContentEvent event) {
    // process AttributeContentEvent by updating the subset of local statistics
    if (event instanceof AttributeBatchContentEvent) {
      AttributeBatchContentEvent abce = (AttributeBatchContentEvent) event;
      List<ContentEvent> contentEventList = abce.getContentEventList();
      for (ContentEvent contentEvent : contentEventList) {
        AttributeContentEvent ace = (AttributeContentEvent) contentEvent;
        Long learningNodeId = ace.getLearningNodeId();
        Integer obsIndex = ace.getObsIndex();

        AttributeClassObserver obs = localStats.get(
            learningNodeId, obsIndex);

        if (obs == null) {
          obs = ace.isNominal() ? newNominalClassObserver()
              : newNumericClassObserver();
          localStats.put(ace.getLearningNodeId(), obsIndex, obs);
        }
        obs.observeAttributeClass(ace.getAttrVal(), ace.getClassVal(),
            ace.getWeight());
      }

      /*
       * if (event instanceof AttributeContentEvent) { AttributeContentEvent ace
       * = (AttributeContentEvent) event; Long learningNodeId =
       * Long.valueOf(ace.getLearningNodeId()); Integer obsIndex =
       * Integer.valueOf(ace.getObsIndex());
       *
       * AttributeClassObserver obs = localStats.get( learningNodeId, obsIndex);
       *
       * if (obs == null) { obs = ace.isNominal() ? newNominalClassObserver() :
       * newNumericClassObserver(); localStats.put(ace.getLearningNodeId(),
       * obsIndex, obs); } obs.observeAttributeClass(ace.getAttrVal(),
       * ace.getClassVal(), ace.getWeight());
       */
    } else if (event instanceof AttributeSliceEvent) {
      AttributeSliceEvent ase = (AttributeSliceEvent) event;
      processAttributeSlice(ase);

    } else if (event instanceof ComputeContentEvent){
      ComputeContentEvent cce = (ComputeContentEvent) event;
      processComputeEvent(cce);

    } else if (event instanceof DeleteContentEvent) {
      DeleteContentEvent dce = (DeleteContentEvent) event;
      Long learningNodeId = dce.getLearningNodeId();
      localStats.rowMap().remove(learningNodeId);
    }
    return true;
  }

  private void processComputeEvent(ComputeContentEvent cce) {
    Long learningNodeId = cce.getLearningNodeId();
    double[] preSplitDist = cce.getPreSplitDist();

    Map<Integer, AttributeClassObserver> learningNodeRowMap = localStats.row(learningNodeId);
    AttributeSplitSuggestion[] suggestions = new AttributeSplitSuggestion[learningNodeRowMap.size()];

    int curIndex = 0;
    for (Entry<Integer, AttributeClassObserver> entry : learningNodeRowMap.entrySet()) {
      AttributeClassObserver obs = entry.getValue();
      AttributeSplitSuggestion suggestion = obs
          .getBestEvaluatedSplitSuggestion(splitCriterion,
              preSplitDist, entry.getKey(), binarySplit);
      if (suggestion == null) {
        suggestion = new AttributeSplitSuggestion();
      }
      suggestions[curIndex] = suggestion;
      curIndex++;
    }

    // Doing this sort instead of keeping the max and second max seems faster for some reason
    Arrays.sort(suggestions);

    AttributeSplitSuggestion bestSuggestion = null;
    AttributeSplitSuggestion secondBestSuggestion = null;

    if (suggestions.length >= 1) {
    bestSuggestion = suggestions[suggestions.length - 1];

      if (suggestions.length >= 2) {
        secondBestSuggestion = suggestions[suggestions.length - 2];
      }
    }

    // create the local result content event
    LocalResultContentEvent lcre =
        new LocalResultContentEvent(cce.getSplitId(), bestSuggestion, secondBestSuggestion);
    lcre.setEnsembleId(cce.getEnsembleId());
    computationResultStream.put(lcre);
  }

  private void processAttributeSlice(AttributeSliceEvent ase) {
    //      System.out.printf("Event with key: %s processed by LSP: %d%n", ase.getKey(), id);
    double[] attributeSlice = ase.getAttributeSlice();
    boolean[] isNominal = ase.getIsNominalSlice();
    int startingIndex = ase.getAttributeStartingIndex();
    Long learningNodeId = ase.getLearningNodeId();
    int classValue = ase.getClassValue();
    double weight = ase.getWeight();

    for (int i = 0; i < attributeSlice.length; i++) {
      Integer obsIndex = i + startingIndex;
      AttributeClassObserver obs = localStats.get(learningNodeId, obsIndex);
      if (obs == null) {
        obs = isNominal[i] ? newNominalClassObserver() : newNumericClassObserver();
        localStats.put(learningNodeId, obsIndex, obs);
      }
      obs.observeAttributeClass(attributeSlice[i], classValue, weight);
    }
  }

  @Override
  public void onCreate(int id) {
    this.id = id;
    this.localStats = HashBasedTable.create();
  }

  @Override
  public Processor newProcessor(Processor p) {
    LocalStatisticsProcessor oldProcessor = (LocalStatisticsProcessor) p;
    LocalStatisticsProcessor newProcessor = new LocalStatisticsProcessor.Builder(oldProcessor).build();

    newProcessor.setComputationResultStream(oldProcessor.getComputationResultStream());

    return newProcessor;
  }

  /**
   * Method to set the computation result when using this processor to build a topology.
   * 
   * @param computeStream
   */
  public void setComputationResultStream(Stream computeStream) {
    this.computationResultStream = computeStream;
  }

  private AttributeClassObserver newNominalClassObserver() {
    return new NominalAttributeClassObserver(); //further investigate this change
  }

  private AttributeClassObserver newNumericClassObserver() {
    return new GaussianNumericAttributeClassObserver();//further investigate this change
  }

  /**
   * Builder class to replace constructors with many parameters
   * 
   * @author Arinto Murdopo
   * 
   */
  public static class Builder {

    private SplitCriterion splitCriterion = new InfoGainSplitCriterion();
    private boolean binarySplit = false;
    private AttributeClassObserver nominalClassObserver = new NominalAttributeClassObserver();
    private AttributeClassObserver numericClassObserver = new GaussianNumericAttributeClassObserver();

    public Builder() {

    }

    public Builder(LocalStatisticsProcessor oldProcessor) {
      this.splitCriterion = oldProcessor.getSplitCriterion();
      this.binarySplit = oldProcessor.isBinarySplit();
      this.nominalClassObserver = oldProcessor.getNominalClassObserver();
      this.numericClassObserver = oldProcessor.getNumericClassObserver();
    }

    public Builder splitCriterion(SplitCriterion splitCriterion) {
      this.splitCriterion = splitCriterion;
      return this;
    }

    public Builder binarySplit(boolean binarySplit) {
      this.binarySplit = binarySplit;
      return this;
    }

    public Builder nominalClassObserver(AttributeClassObserver nominalClassObserver) {
      this.nominalClassObserver = nominalClassObserver;
      return this;
    }

    public Builder numericClassObserver(AttributeClassObserver numericClassObserver) {
      this.numericClassObserver = numericClassObserver;
      return this;
    }

    public LocalStatisticsProcessor build() {
      return new LocalStatisticsProcessor(this);
    }
  }
  
  public SplitCriterion getSplitCriterion() {
    return splitCriterion;
  }
  
  public boolean isBinarySplit() {
    return binarySplit;
  }
  
  public AttributeClassObserver getNominalClassObserver() {
    return nominalClassObserver;
  }
  
  public AttributeClassObserver getNumericClassObserver() {
    return numericClassObserver;
  }
  
  public Stream getComputationResultStream() {
    return computationResultStream;
  }
}
