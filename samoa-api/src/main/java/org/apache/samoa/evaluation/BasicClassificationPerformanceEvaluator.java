package org.apache.samoa.evaluation;

import java.util.List;

import org.apache.samoa.instances.Attribute;

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

import org.apache.samoa.instances.instances.Instance;
import org.apache.samoa.instances.Utils;
import org.apache.samoa.moa.AbstractMOAObject;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.core.Vote;

/**
 * Classification evaluator that performs basic incremental evaluation.
 * 
 * @author Richard Kirkby (rkirkby@cs.waikato.ac.nz)
 * @author Albert Bifet (abifet at cs dot waikato dot ac dot nz)
 * @version $Revision: 7 $
 */
public class BasicClassificationPerformanceEvaluator extends AbstractMOAObject
    implements ClassificationPerformanceEvaluator {

  private static final long serialVersionUID = 1L;

  // the number of decimal places placed for double values in prediction file
  // the value of 10 is used since some votes can be relatively small
  public static final int DECIMAL_PLACES = 10;

  // the vote value to be used when a classifier made no vote for the class at
  // all
  public static final int NO_VOTE_FOR_CLASS = 0;

  // recent vote objects i.e. predicted, true classes and votes for individual
  // classes
  protected Vote[] votes;

  protected double weightObserved;

  protected double weightCorrect;

  protected double[] columnKappa;

  protected double[] rowKappa;

  protected int numClasses;

  private double weightCorrectNoChangeClassifier;

  protected double[] classVotes;

  private int lastSeenClass;
  private String instanceIdentifier;

  private Instance lastSeenInstance;

  @Override
  public void reset() {
    reset(this.numClasses);
    votes = null;
  }

  public void reset(int numClasses) {
    this.numClasses = numClasses;
    this.rowKappa = new double[numClasses];
    this.columnKappa = new double[numClasses];
    for (int i = 0; i < this.numClasses; i++) {
      this.rowKappa[i] = 0.0;
      this.columnKappa[i] = 0.0;
    }
    this.weightObserved = 0.0;
    this.weightCorrect = 0.0;
    this.weightCorrectNoChangeClassifier = 0.0;
    this.lastSeenClass = 0;
    votes = null;
  }

  @Override
  public void addResult(Instance inst, double[] classVotes, String instanceIdentifier) {
    double weight = inst.weight();
    int trueClass = (int) inst.classValue();
    if (weight > 0.0) {
      if (this.weightObserved == 0) {
        reset(inst.numClasses());
      }
      this.weightObserved += weight;
      int predictedClass = Utils.maxIndex(classVotes);
      if (predictedClass == trueClass) {
        this.weightCorrect += weight;
      }
      if (rowKappa.length > 0) {
        this.rowKappa[predictedClass] += weight;
      }
      if (columnKappa.length > 0) {
        this.columnKappa[trueClass] += weight;
      }
    }
    if (this.lastSeenClass == trueClass) {
      this.weightCorrectNoChangeClassifier += weight;
    }
    this.lastSeenClass = trueClass;
    this.lastSeenInstance = inst;
    this.instanceIdentifier = instanceIdentifier;
    this.classVotes = classVotes;
  }

  @Override
  public Measurement[] getPerformanceMeasurements() {
    return new Measurement[] { new Measurement("classified instances", getTotalWeightObserved()),
        new Measurement("classifications correct (percent)", getFractionCorrectlyClassified() * 100.0),
        new Measurement("Kappa Statistic (percent)", getKappaStatistic() * 100.0),
        new Measurement("Kappa Temporal Statistic (percent)", getKappaTemporalStatistic() * 100.0) };

  }

  /**
   * This method is used to retrieve predictions and votes (for classification only)
   * 
   * @return String This returns an array of predictions and votes objects.
   */
  @Override
  public Vote[] getPredictionVotes() {
    Attribute classAttribute = this.lastSeenInstance.dataset().classAttribute();
    double trueValue = this.lastSeenInstance.classValue();
    List<String> classAttributeValues = classAttribute.getAttributeValues();

    int trueNominalIndex = (int) trueValue;
    String trueNominalValue = classAttributeValues.get(trueNominalIndex);

    // initialise votes first time they are supposed to be used
    if (votes == null) {
      this.votes = new Vote[classAttributeValues.size() + 3];
      votes[0] = new Vote("instance number");
      votes[1] = new Vote("true class value");
      votes[2] = new Vote("predicted class value");

      // create as many objects as the number of classes
      for (int i = 0; i < classAttributeValues.size(); i++) {
        votes[3 + i] = new Vote("votes_" + classAttributeValues.get(i));
      }
    }

    // use/(re-use existing) vote objects
    votes[0].setValue(this.instanceIdentifier);
    votes[1].setValue(trueNominalValue);
    votes[2].setValue(classAttributeValues.get(Utils.maxIndex(classVotes)));
    for (int i = 0; i < classAttributeValues.size(); i++) {
      if (i < classVotes.length) {
        votes[3 + i].setValue(classVotes[i], this.DECIMAL_PLACES);
      } else {
        votes[3 + i].setValue(this.NO_VOTE_FOR_CLASS, 0);
      }
    }

    return votes;

  }

  public double getTotalWeightObserved() {
    return this.weightObserved;
  }

  public double getFractionCorrectlyClassified() {
    return this.weightObserved > 0.0 ? this.weightCorrect / this.weightObserved : 0.0;
  }

  public double getFractionIncorrectlyClassified() {
    return 1.0 - getFractionCorrectlyClassified();
  }

  public double getKappaStatistic() {
    if (this.weightObserved > 0.0) {
      double p0 = getFractionCorrectlyClassified();
      double pc = 0.0;
      for (int i = 0; i < this.numClasses; i++) {
        pc += (this.rowKappa[i] / this.weightObserved) * (this.columnKappa[i] / this.weightObserved);
      }
      return (p0 - pc) / (1.0 - pc);
    } else {
      return 0;
    }
  }

  public double getKappaTemporalStatistic() {
    if (this.weightObserved > 0.0) {
      double p0 = this.weightCorrect / this.weightObserved;
      double pc = this.weightCorrectNoChangeClassifier / this.weightObserved;

      return (p0 - pc) / (1.0 - pc);
    } else {
      return 0;
    }
  }

  @Override
  public void getDescription(StringBuilder sb, int indent) {
    Measurement.getMeasurementsDescription(getPerformanceMeasurements(), sb, indent);
  }
}
