package org.apache.samoa.moa.classifiers.core.driftdetection;

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

import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.tasks.TaskMonitor;

import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;

/**
 * Drift detection method based in Page Hinkley Test.
 * 
 * 
 * @author Manuel Baena (mbaena@lcc.uma.es)
 * @version $Revision: 7 $
 */
public class PageHinkleyDM extends AbstractChangeDetector {

  private static final long serialVersionUID = -3518369648142099719L;

  public IntOption minNumInstancesOption = new IntOption(
      "minNumInstances",
      'n',
      "The minimum number of instances before permitting detecting change.",
      30, 0, Integer.MAX_VALUE);

  public FloatOption deltaOption = new FloatOption("delta", 'd',
      "Delta parameter of the Page Hinkley Test", 0.005, 0.0, 1.0);

  public FloatOption lambdaOption = new FloatOption("lambda", 'l',
      "Lambda parameter of the Page Hinkley Test", 50, 0.0, Float.MAX_VALUE);

  public FloatOption alphaOption = new FloatOption("alpha", 'a',
      "Alpha parameter of the Page Hinkley Test", 1 - 0.0001, 0.0, 1.0);

  private int m_n;

  private double sum;

  private double x_mean;

  private double alpha;

  private double delta;

  private double lambda;

  public PageHinkleyDM() {
    resetLearning();
  }

  @Override
  public void resetLearning() {
    m_n = 1;
    x_mean = 0.0;
    sum = 0.0;
    delta = this.deltaOption.getValue();
    alpha = this.alphaOption.getValue();
    lambda = this.lambdaOption.getValue();
  }

  @Override
  public void input(double x) {
    // It monitors the error rate
    if (this.isChangeDetected) {
      resetLearning();
    }

    x_mean = x_mean + (x - x_mean) / (double) m_n;
    sum = this.alpha * sum + (x - x_mean - this.delta);
    m_n++;
    this.estimation = x_mean;
    this.isChangeDetected = false;
    this.isWarningZone = false;
    this.delay = 0;

    if (m_n < this.minNumInstancesOption.getValue()) {
      return;
    }

    if (sum > this.lambda) {
      this.isChangeDetected = true;
    }
  }

  @Override
  public void getDescription(StringBuilder sb, int indent) {
    // TODO Auto-generated method stub
  }

  @Override
  protected void prepareForUseImpl(TaskMonitor monitor,
      ObjectRepository repository) {
    // TODO Auto-generated method stub
  }
}
