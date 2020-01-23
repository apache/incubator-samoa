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

package org.apache.samoa.moa.classifiers.functions;

import com.github.javacliparser.FlagOption;
import com.github.javacliparser.IntOption;
import org.apache.samoa.moa.classifiers.AbstractClassifier;
import org.apache.samoa.moa.classifiers.core.AttributeSplitSuggestion;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.AttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.GaussianNumericAttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.attributeclassobservers.NominalAttributeClassObserver;
import org.apache.samoa.moa.classifiers.core.splitcriteria.SplitCriterion;
import org.apache.samoa.moa.core.AutoExpandVector;
import org.apache.samoa.moa.core.DoubleVector;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.options.ClassOption;
import org.apache.samoa.instances.Instance;

/**
 * Decision trees of one level.<br />
 *
 * Parameters:</p>
 * <ul>
 * <li>-g : The number of instances to observe between model changes</li>
 * <li>-b : Only allow binary splits</li>
 * <li>-c : Split criterion to use. Example : InfoGainSplitCriterion</li>
 * <li>-r : Seed for random behaviour of the classifier</li>
 * </ul>
 *
 * @author Richard Kirkby (rkirkby@cs.waikato.ac.nz)
 * @version $Revision: 7 $
 */
public class DecisionStump extends AbstractClassifier {

  private static final long serialVersionUID = 1L;

  @Override
  public String getPurposeString() {
    return "Decision trees of one level.";
  }

  public IntOption gracePeriodOption = new IntOption("gracePeriod", 'g',
      "The number of instances to observe between model changes.", 1000,
      0, Integer.MAX_VALUE);

  public FlagOption binarySplitsOption = new FlagOption("binarySplits", 'b',
      "Only allow binary splits.");

  public ClassOption splitCriterionOption = new ClassOption("splitCriterion",
      'c', "Split criterion to use.", SplitCriterion.class,
      "InfoGainSplitCriterion");

  protected AttributeSplitSuggestion bestSplit;

  protected DoubleVector observedClassDistribution;

  protected AutoExpandVector<AttributeClassObserver> attributeObservers;

  protected double weightSeenAtLastSplit;

  @Override
  public void resetLearningImpl() {
    this.bestSplit = null;
    this.observedClassDistribution = new DoubleVector();
    this.attributeObservers = new AutoExpandVector<AttributeClassObserver>();
    this.weightSeenAtLastSplit = 0.0;
  }

  @Override
  protected Measurement[] getModelMeasurementsImpl() {
    return null;
  }

  @Override
  public void getModelDescription(StringBuilder out, int indent) {
    // TODO Auto-generated method stub
  }

  @Override
  public void trainOnInstanceImpl(Instance inst) {
    this.observedClassDistribution.addToValue((int) inst.classValue(), inst.weight());
    for (int i = 0; i < inst.numAttributes() - 1; i++) {
      int instAttIndex = modelAttIndexToInstanceAttIndex(i);
      AttributeClassObserver obs = this.attributeObservers.get(i);
      if (obs == null) {
        obs = inst.attribute(instAttIndex).isNominal() ? newNominalClassObserver()
            : newNumericClassObserver();
        this.attributeObservers.set(i, obs);
      }
      obs.observeAttributeClass(inst.value(instAttIndex), (int) inst.classValue(), inst.weight());
    }
    if (this.trainingWeightSeenByModel - this.weightSeenAtLastSplit >= this.gracePeriodOption.getValue()) {
      this.bestSplit = findBestSplit((SplitCriterion) getPreparedClassOption(this.splitCriterionOption));
      this.weightSeenAtLastSplit = this.trainingWeightSeenByModel;
    }
  }

  @Override
  public double[] getVotesForInstance(Instance inst) {
    if (this.bestSplit != null) {
      int branch = this.bestSplit.splitTest.branchForInstance(inst);
      if (branch >= 0) {
        return this.bestSplit.resultingClassDistributionFromSplit(branch);
      }
    }
    return this.observedClassDistribution.getArrayCopy();
  }

  @Override
  public boolean isRandomizable() {
    return false;
  }

  protected AttributeClassObserver newNominalClassObserver() {
    return new NominalAttributeClassObserver();
  }

  protected AttributeClassObserver newNumericClassObserver() {
    return new GaussianNumericAttributeClassObserver();
  }

  protected AttributeSplitSuggestion findBestSplit(SplitCriterion criterion) {
    AttributeSplitSuggestion bestFound = null;
    double bestMerit = Double.NEGATIVE_INFINITY;
    double[] preSplitDist = this.observedClassDistribution.getArrayCopy();
    for (int i = 0; i < this.attributeObservers.size(); i++) {
      AttributeClassObserver obs = this.attributeObservers.get(i);
      if (obs != null) {
        AttributeSplitSuggestion suggestion = obs.getBestEvaluatedSplitSuggestion(criterion,
            preSplitDist, i, this.binarySplitsOption.isSet());
        if (suggestion.merit > bestMerit) {
          bestMerit = suggestion.merit;
          bestFound = suggestion;
        }
      }
    }
    return bestFound;
  }
}
