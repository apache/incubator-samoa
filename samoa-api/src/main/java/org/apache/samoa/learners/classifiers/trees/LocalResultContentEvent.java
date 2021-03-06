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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.moa.classifiers.core.AttributeSplitSuggestion;

/**
 * Local Result Content Event is the content event that represents local calculation of statistic in Local Statistic
 * Processor.
 * 
 * @author Arinto Murdopo
 * 
 */
public final class LocalResultContentEvent implements ContentEvent {

  private static final long serialVersionUID = -4206620993777418571L;

  private final AttributeSplitSuggestion bestSuggestion;
  private final AttributeSplitSuggestion secondBestSuggestion;
  private final long splitId;
  private int ensembleId; //the id of the ensemble that asked for the local statistics

  public LocalResultContentEvent() {
    bestSuggestion = null;
    secondBestSuggestion = null;
    splitId = -1;
  }

  LocalResultContentEvent(long splitId, AttributeSplitSuggestion best, AttributeSplitSuggestion secondBest) {
    this.splitId = splitId;
    this.bestSuggestion = best;
    this.secondBestSuggestion = secondBest;
  }

  @Override
  public String getKey() {
    return null;
  }

  /**
   * Method to return the best attribute split suggestion from this local statistic calculation.
   * 
   * @return The best attribute split suggestion.
   */
  public AttributeSplitSuggestion getBestSuggestion() {
    return this.bestSuggestion;
  }

  /**
   * Method to return the second best attribute split suggestion from this local statistic calculation.
   * 
   * @return The second best attribute split suggestion.
   */
  public AttributeSplitSuggestion getSecondBestSuggestion() {
    return this.secondBestSuggestion;
  }

  /**
   * Method to get the split ID of this local statistic calculation result
   * 
   * @return The split id of this local calculation result
   */
  public long getSplitId() {
    return this.splitId;
  }

  @Override
  public void setKey(String str) {
    // do nothing

  }

  @Override
  public boolean isLastEvent() {
    return false;
  }

  public int getEnsembleId() {
    return ensembleId;
  }
  
  public void setEnsembleId(int ensembleId) {
    this.ensembleId = ensembleId;
  }
}
