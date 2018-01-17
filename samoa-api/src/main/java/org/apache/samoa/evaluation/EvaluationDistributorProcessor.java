package org.apache.samoa.evaluation;

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

/**
 * License
 */

import com.google.common.base.Preconditions;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.instances.Instance;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.moa.core.MiscUtils;
import org.apache.samoa.topology.Stream;

import java.util.Arrays;
import java.util.Random;

/**
 * The Class EvaluationDistributorProcessor.
 */
public class EvaluationDistributorProcessor implements Processor {

  private static final long serialVersionUID = -1550901409625192734L;

  /** The ensemble size or number of folds. */
  private int numberClassifiers;

  /** The stream ensemble. */
  private Stream[] ensembleStreams;

  /** Random number generator. */
  protected Random random = new Random();

  /** Random seed */
  protected int randomSeed;

  /** The methodology to use to perform the validation */
  public int validationMethodology;

  /**
   * On event.
   * 
   * @param event
   *          the event
   * @return true, if successful
   */
  public boolean process(ContentEvent event) {
    Preconditions.checkState(numberClassifiers == ensembleStreams.length, String.format(
        "Ensemble size ({}) and number of ensemble streams ({}) do not match.", numberClassifiers, ensembleStreams.length));
    InstanceContentEvent inEvent = (InstanceContentEvent) event;

    if (inEvent.getInstanceIndex() < 0) {
      // end learning
      for (Stream stream : ensembleStreams)
        stream.put(event);
      return false;
    }

    if (inEvent.isTesting()) {
      Instance testInstance = inEvent.getInstance();
      for (int i = 0; i < numberClassifiers; i++) {
        Instance instanceCopy = testInstance.copy();
        InstanceContentEvent instanceContentEvent = new InstanceContentEvent(inEvent.getInstanceIndex(), instanceCopy,
            false, true);
        instanceContentEvent.setEvaluationIndex(i); //TODO probably not needed anymore
        ensembleStreams[i].put(instanceContentEvent);
      }
    }

    // estimate model parameters using the training data
    if (inEvent.isTraining()) {
      train(inEvent);
    }
    return true;
  }

  /**
   * Train.
   * 
   * @param inEvent
   *          the in event
   */
  protected void train(InstanceContentEvent inEvent) {
    Instance trainInstance = inEvent.getInstance();
    long instancesProcessed = inEvent.getInstanceIndex();
    for (int i = 0; i < numberClassifiers; i++) {
      int k = 1;
      switch (this.validationMethodology) {
        case 0: //Cross-Validation;
          k = instancesProcessed % numberClassifiers == i ? 0 : 1; //Test all except one
          break;
        case 1: //Bootstrap;
          k = MiscUtils.poisson(1, this.random);
          break;
        case 2: //Split-Validation;
          k = instancesProcessed % numberClassifiers == i ? 1 : 0; //Test only one
          break;
      }
      if (k > 0) {
        Instance weightedInstance = trainInstance.copy();
        weightedInstance.setWeight(trainInstance.weight() * k);
        InstanceContentEvent instanceContentEvent = new InstanceContentEvent(inEvent.getInstanceIndex(),
            weightedInstance, true, false);
        instanceContentEvent.setEvaluationIndex(i);
        ensembleStreams[i].put(instanceContentEvent);
      }
    }
  }

  @Override
  public void onCreate(int id) {
    // do nothing
  }

  public Stream[] getOutputStreams() {
    return ensembleStreams;
  }

  public void setOutputStreams(Stream[] ensembleStreams) {
    this.ensembleStreams = ensembleStreams;
  }

  public int getNumberClassifiers() {
    return numberClassifiers;
  }

  public void setNumberClassifiers(int numberClassifiers) {
    this.numberClassifiers = numberClassifiers;
  }

  public void setValidationMethodologyOption(int index) { this.validationMethodology = index;}

  public void setRandomSeed(int seed){this.randomSeed = seed; this.random = new Random(seed);}

  @Override
  public Processor newProcessor(Processor sourceProcessor) {
    EvaluationDistributorProcessor newProcessor = new EvaluationDistributorProcessor();
    EvaluationDistributorProcessor originProcessor = (EvaluationDistributorProcessor) sourceProcessor;
    if (originProcessor.getOutputStreams() != null) {
      newProcessor.setOutputStreams(Arrays.copyOf(originProcessor.getOutputStreams(),
          originProcessor.getOutputStreams().length));
    }
    newProcessor.setNumberClassifiers(originProcessor.getNumberClassifiers());
    newProcessor.setValidationMethodologyOption(originProcessor.validationMethodology);
    newProcessor.setRandomSeed(originProcessor.randomSeed);
    return newProcessor;
  }
}
