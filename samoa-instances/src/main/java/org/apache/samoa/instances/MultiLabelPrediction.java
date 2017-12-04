package org.apache.samoa.instances;

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

import java.io.Serializable;
import java.util.ArrayList;

public class MultiLabelPrediction implements Prediction, Serializable {
  protected ArrayList< ArrayList<Double> > prediction;

  public MultiLabelPrediction() {
    this(0);
  }

  public MultiLabelPrediction(int numOutputAttributes) {
    prediction = new ArrayList< ArrayList<Double> >();
    for (int i = 0; i < numOutputAttributes; i++)
      prediction.add(new ArrayList<Double>());
  }

  @Override
  public int numOutputAttributes() {
    return prediction.size();
  }

  @Override
  public int numClasses(int outputAttributeIndex) {
    int ret = 0;
    if (prediction.size() > outputAttributeIndex) {
      ret =  prediction.get(outputAttributeIndex).size();
    }
    return ret;
  }

  @Override
  public double[] getVotes(int outputAttributeIndex) {
    int s = prediction.get(outputAttributeIndex).size();
    double ret[] = null;
    if (prediction.size() > outputAttributeIndex) {
      ArrayList<Double> aux = prediction.get(outputAttributeIndex);
      ret = new double[s];
      for (int i = 0; i < s; i++) {
        ret[i] = aux.get(i).doubleValue();
      }
    }

    return ret;
  }

  @Override
  public double[] getVotes() {
    return getVotes(0);
  }

  @Override
  public double getVote(int outputAttributeIndex, int classIndex) {
    double ret = 0.0;
    if (prediction.size() > outputAttributeIndex) {
      ret = (classIndex >= 0 && classIndex < prediction.get(outputAttributeIndex).size()) ?
          prediction.get(outputAttributeIndex).get(classIndex) : 0;
    }
    return ret;
  }

  @Override
  public void setVotes(int outputAttributeIndex, double[] votes) {
    for(int i = 0; i < votes.length; i++) {
      if (i >= prediction.get(outputAttributeIndex).size()) {
        prediction.get(outputAttributeIndex).ensureCapacity(i+1);
        while (prediction.get(outputAttributeIndex).size() < i + 1) {
          prediction.get(outputAttributeIndex).add(0.0);
        }
      }

      prediction.get(outputAttributeIndex).set(i,votes[i]);
    }
  }

  @Override
  public void setVotes(double[] votes) {
    setVotes(0, votes);
  }

  @Override
  public void setVote(int outputAttributeIndex, int classIndex, double vote) {
    if (outputAttributeIndex >= prediction.get(outputAttributeIndex).size()) {
      prediction.get(outputAttributeIndex).ensureCapacity(classIndex + 1);
      while (prediction.get(outputAttributeIndex).size() < classIndex + 1) {
        prediction.get(outputAttributeIndex).add(0.0);
      }
    }

    prediction.get(outputAttributeIndex).set(classIndex, vote);
  }

  @Override
  public String toString(){
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < prediction.size(); i++){
      sb.append("Out " + i + ": ");
      for (int c = 0; c<prediction.get(i).size(); c++)
      {
        sb.append(((int)(prediction.get(i).get(c) * 1000) / 1000.0) + " ");
      }
    }
    return sb.toString();
  }

  @Override
  public boolean hasVotesForAttribute(int outputAttributeIndex) {
    if(prediction.size() < (outputAttributeIndex + 1))
      return false;
    return (prediction.get(outputAttributeIndex).size() == 0) ? false : true;
  }

  @Override
  public int size() {
    return prediction.size();
  }

}