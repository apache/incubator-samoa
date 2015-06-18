package org.apache.samoa.moa.clusterers;

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

import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.moa.MOAObject;
import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.options.OptionHandler;

public interface Clusterer extends MOAObject, OptionHandler {

  public void setModelContext(InstancesHeader ih);

  public InstancesHeader getModelContext();

  public boolean isRandomizable();

  public void setRandomSeed(int s);

  public boolean trainingHasStarted();

  public double trainingWeightSeenByModel();

  public void resetLearning();

  public void trainOnInstance(Instance inst);

  public double[] getVotesForInstance(Instance inst);

  public Measurement[] getModelMeasurements();

  public Clusterer[] getSubClusterers();

  public Clusterer copy();

  public Clustering getClusteringResult();

  public boolean implementsMicroClusterer();

  public Clustering getMicroClusteringResult();

  public boolean keepClassLabel();

}
