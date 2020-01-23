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

package org.apache.samoa.tasks;

import com.github.javacliparser.*;
import org.apache.samoa.evaluation.*;
import org.apache.samoa.learners.ClassificationLearner;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.RegressionLearner;
import org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.generators.RandomTreeGenerator;
import org.apache.samoa.streams.PrequentialSourceProcessor;
import org.apache.samoa.topology.ComponentFactory;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Prequential Cross-Validation Evaluation. Evaluation that evaluates performance of online
 * classifiers using prequential cross-validation: each instance is used for testing online
 * classifiers model and then the same instance is used for training the model using one of
 * these strategies: k-fold distributed Cross Validation, k-fold distributed Bootstrap Validation,
 * or k-fold distributed Split Validation.
 *
 * More information in: Albert Bifet, Gianmarco De Francisci Morales, Jesse Read, Geoff Holmes,
 *  Bernhard Pfahringer: Efficient Online Evaluation of Big Data Stream Classifiers. KDD 2015: 59-68
 * 
 */
public class PrequentialCVEvaluation extends PrequentialEvaluation {

  public IntOption foldNumberOption = new IntOption("foldNumber", 'x',
          "The number of distributed models.", 10, 1, Integer.MAX_VALUE);

  public MultiChoiceOption validationMethodologyOption = new MultiChoiceOption(
          "validationMethodology", 'a', "Validation methodology to use.", new String[]{
          "Cross-Validation", "Bootstrap-Validation", "Split-Validation"},
          new String[]{"k-fold distributed Cross Validation",
                  "k-fold distributed Bootstrap Validation",
                  "k-fold distributed Split Validation"
          }, 0);

  public IntOption randomSeedOption = new IntOption("randomSeed", 'r',
          "Seed for random behaviour of the task.", 1);

  public void getDescription(StringBuilder sb, int indent) {
    sb.append("Prequential CV evaluation");
  }

  /** The distributor processor. */
  private EvaluationDistributorProcessor distributorP;

  private Stream[] ensembleStream;

  protected Learner[] ensemble;

  private EvaluatorCVProcessor evaluator; 

  private static Logger logger = LoggerFactory.getLogger(PrequentialCVEvaluation.class);

  @Override
  public void init() {
    // TODO remove the if statement
    // theoretically, dynamic binding will work here!
    // test later!
    // for now, the if statement is used by Storm

    if (builder == null) {
      builder = new TopologyBuilder();
      logger.debug("Successfully instantiating TopologyBuilder");

      builder.initTopology(evaluationNameOption.getValue());
      logger.debug("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());
    }

    // instantiate PrequentialSourceProcessor and its output stream
    // (sourcePiOutputStream)
    preqSource = new PrequentialSourceProcessor();
    preqSource.setStreamSource((InstanceStream) this.streamTrainOption.getValue());
    preqSource.setMaxNumInstances(instanceLimitOption.getValue());
    preqSource.setSourceDelay(sourceDelayOption.getValue());
    preqSource.setDelayBatchSize(batchDelayOption.getValue());
    builder.addEntranceProcessor(preqSource);
    logger.debug("Successfully instantiating PrequentialSourceProcessor");

    sourcePiOutputStream = builder.createStream(preqSource);

    //Add EvaluationDistributorProcessor
    int numberFolds = this.foldNumberOption.getValue();
    distributorP = new EvaluationDistributorProcessor();
    distributorP.setNumberClassifiers(numberFolds);
    distributorP.setValidationMethodologyOption(this.validationMethodologyOption.getChosenIndex());
    distributorP.setRandomSeed(this.randomSeedOption.getValue());
    builder.addProcessor(distributorP, 1);
    builder.connectInputAllStream(sourcePiOutputStream, distributorP);

    // instantiate classifier
    int foldNumber = this.foldNumberOption.getValue();
    ensemble = new Learner[foldNumber];
    for (int i = 0; i < foldNumber; i++) {
      try {
        ensemble[i] = (Learner) ClassOption.createObject(learnerOption.getValueAsCLIString(),
                learnerOption.getRequiredType());
      } catch (Exception e) {
        logger.error("Unable to create classifiers for the distributed evaluation. Please check your CLI parameters");
        e.printStackTrace();
        throw new IllegalArgumentException(e);
      }
      ensemble[i].init(builder, preqSource.getDataset(), 1); // sequential
    }
    logger.debug("Successfully instantiating Classifiers");

    Stream[] ensembleStreams = new Stream[foldNumber];
    for (int i = 0; i < foldNumber; i++) {
      ensembleStreams[i] = builder.createStream(distributorP);
      builder.connectInputShuffleStream(ensembleStreams[i], ensemble[i].getInputProcessor()); // connect streams one-to-one with ensemble members (the type of connection does not matter)
    }
    distributorP.setOutputStreams(ensembleStreams);

    PerformanceEvaluator evaluatorOptionValue = this.evaluatorOption.getValue();
    if (!PrequentialCVEvaluation.isLearnerAndEvaluatorCompatible(ensemble[0], evaluatorOptionValue)) {
      evaluatorOptionValue = getDefaultPerformanceEvaluatorForLearner(ensemble[0]);
    }
    evaluator = new EvaluatorCVProcessor.Builder(evaluatorOptionValue)
            .samplingFrequency(sampleFrequencyOption.getValue())
            .dumpFile(dumpFileOption.getFile())
            .foldNumber(numberFolds).build();

    builder.addProcessor(evaluator, 1);

    for (Learner member : ensemble) {
      for (Stream subResultStream : member.getResultStreams()) { // a learner can have multiple output streams
        this.builder.connectInputKeyStream(subResultStream, evaluator); // the key is the instance id to combine predictions
      }
    }

    logger.debug("Successfully instantiating EvaluatorProcessor");

    prequentialTopology = builder.build();
    logger.debug("Successfully building the topology");
  }

}
