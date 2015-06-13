package org.apache.samoa.moa.classifiers.rules.driftdetection;

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

/**
 * Page-Hinkley Test with equal weights for all instances.
 * 
 */
public class PageHinkleyTest implements Serializable {

  private static final long serialVersionUID = 1L;
  protected double cumulativeSum;

  public double getCumulativeSum() {
    return cumulativeSum;
  }

  public double getMinimumValue() {
    return minimumValue;
  }

  protected double minimumValue;
  protected double sumAbsolutError;
  protected long phinstancesSeen;
  protected double threshold;
  protected double alpha;

  public PageHinkleyTest() {
    this(0, 0);
  }

  public PageHinkleyTest(double threshold, double alpha) {
    this.threshold = threshold;
    this.alpha = alpha;
    this.reset();
  }

  public void reset() {
    this.cumulativeSum = 0.0;
    this.minimumValue = Double.MAX_VALUE;
    this.sumAbsolutError = 0.0;
    this.phinstancesSeen = 0;
  }

  // Compute Page-Hinkley test
  public boolean update(double error) {

    this.phinstancesSeen++;
    double absolutError = Math.abs(error);
    this.sumAbsolutError = this.sumAbsolutError + absolutError;
    if (this.phinstancesSeen > 30) {
      double mT = absolutError - (this.sumAbsolutError / this.phinstancesSeen) - this.alpha;
      this.cumulativeSum = this.cumulativeSum + mT; // Update the cumulative mT sum
      if (this.cumulativeSum < this.minimumValue) { // Update the minimum mT value if the new mT is smaller than the current minimum
        this.minimumValue = this.cumulativeSum;
      }
      return (((this.cumulativeSum - this.minimumValue) > this.threshold));
    }
    return false;
  }

  public PageHinkleyTest getACopy() {
    PageHinkleyTest newTest = new PageHinkleyTest(this.threshold, this.alpha);
    this.copyFields(newTest);
    return newTest;
  }

  protected void copyFields(PageHinkleyTest newTest) {
    newTest.cumulativeSum = this.cumulativeSum;
    newTest.minimumValue = this.minimumValue;
    newTest.sumAbsolutError = this.sumAbsolutError;
    newTest.phinstancesSeen = this.phinstancesSeen;
  }

}
