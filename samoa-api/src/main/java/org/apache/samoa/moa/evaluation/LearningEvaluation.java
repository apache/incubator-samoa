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

package org.apache.samoa.moa.evaluation;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.samoa.moa.AbstractMOAObject;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.learners.Learner;

/**
 * Class that stores an array of evaluation measurements.
 * 
 * @author Richard Kirkby (rkirkby@cs.waikato.ac.nz)
 * @version $Revision: 7 $
 */
public class LearningEvaluation extends AbstractMOAObject {

  private static final long serialVersionUID = 1L;

  protected Measurement[] measurements;

  public LearningEvaluation(Measurement[] measurements) {
    this.measurements = measurements.clone();
  }

  public LearningEvaluation(Measurement[] evaluationMeasurements,
      LearningPerformanceEvaluator cpe, Learner model) {
    List<Measurement> measurementList = new LinkedList<>();
    measurementList.addAll(Arrays.asList(evaluationMeasurements));
    measurementList.addAll(Arrays.asList(cpe.getPerformanceMeasurements()));
    measurementList.addAll(Arrays.asList(model.getModelMeasurements()));
    this.measurements = measurementList.toArray(new Measurement[measurementList.size()]);
  }

  public Measurement[] getMeasurements() {
    return this.measurements.clone();
  }

  @Override
  public void getDescription(StringBuilder sb, int indent) {
    Measurement.getMeasurementsDescription(this.measurements, sb, indent);
  }
}
