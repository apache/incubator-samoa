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

package org.apache.samoa.instances;

import java.io.Serializable;

public class MultiLabelPrediction implements Prediction, Serializable {
  protected DoubleVector[] prediction;

  public MultiLabelPrediction() {
    this(0);
  }

  public MultiLabelPrediction(int numOutputAttributes) {
    prediction = new DoubleVector[numOutputAttributes];
    for (int i = 0; i < numOutputAttributes; i++)
      prediction[i] = new DoubleVector();
  }

  @Override
  public int numOutputAttributes() {
    return prediction.length;
  }

  @Override
  public int numClasses(int outputAttributeIndex) {
    int ret = 0;
    if (prediction.length > outputAttributeIndex) {
      ret = prediction[outputAttributeIndex].numValues();
    }
    return ret;
  }

  @Override
  public double[] getVotes(int outputAttributeIndex) {
    double ret[] = null;
    if (prediction.length > outputAttributeIndex) {
      ret = prediction[outputAttributeIndex].getArrayCopy();
    }
    return ret;
  }

  @Override
  public double[] getVotes() {
    return getVotes(0);
  }

  @Override
  public void setVotes(double[] votes) {
    setVotes(0, votes);
  }

  @Override
  public double getVote(int outputAttributeIndex, int classIndex) {
    double ret = 0.0;
    if (prediction.length > outputAttributeIndex) {
      ret = prediction[outputAttributeIndex].getValue(classIndex);
    }
    return ret;
  }

  @Override
  public void setVotes(int outputAttributeIndex, double[] votes) {
    for (int i = 0; i < votes.length; i++)
      prediction[outputAttributeIndex].setValue(i, votes[i]);
  }

  @Override
  public void setVote(int outputAttributeIndex, int classIndex, double vote) {
    prediction[outputAttributeIndex].setValue(classIndex, vote);

  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < prediction.length; i++) {
      sb.append("Out " + i + ": ");
      for (int c = 0; c < prediction[i].numValues(); c++) {
        sb.append(((int) (prediction[i].getValue(c) * 1000) / 1000.0) + " ");
      }
    }
    return sb.toString();
  }

  @Override
  public boolean hasVotesForAttribute(int outputAttributeIndex) {
    if (prediction.length < (outputAttributeIndex + 1))
      return false;
    return (prediction[outputAttributeIndex].numValues() == 0) ? false : true;
  }

  @Override
  public int size() {
    return prediction.length;
  }

  protected class DoubleVector implements Serializable {

    private static final long serialVersionUID = 1L;

    protected double[] array;

    public DoubleVector() {
      this.array = new double[0];
    }

    public DoubleVector(double[] toCopy) {
      this.array = new double[toCopy.length];
      System.arraycopy(toCopy, 0, this.array, 0, toCopy.length);
    }

    public int numValues() {
      return this.array.length;
    }

    public void setValue(int i, double v) {
      if (i >= this.array.length) {
        setArrayLength(i + 1);
      }
      this.array[i] = v;
    }

    public void addToValue(int i, double v) {
      if (i >= this.array.length) {
        setArrayLength(i + 1);
      }
      this.array[i] += v;
    }

    // returns 0.0 for values outside of range
    public double getValue(int i) {
      return ((i >= 0) && (i < this.array.length)) ? this.array[i] : 0.0;
    }


    public int maxIndex() {
      int max = -1;
      for (int i = 0; i < this.array.length; i++) {
        if ((max < 0) || (this.array[i] > this.array[max])) {
          max = i;
        }
      }
      return max;
    }

    public double[] getArrayCopy() {
      double[] aCopy = new double[this.array.length];
      System.arraycopy(this.array, 0, aCopy, 0, this.array.length);
      return aCopy;
    }

    protected void setArrayLength(int l) {
      double[] newArray = new double[l];
      int numToCopy = this.array.length;
      if (numToCopy > l) {
        numToCopy = l;
      }
      System.arraycopy(this.array, 0, newArray, 0, numToCopy);
      this.array = newArray;
    }


  }

}
