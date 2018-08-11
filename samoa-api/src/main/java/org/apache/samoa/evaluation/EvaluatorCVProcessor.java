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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.evaluation.LearningCurve;
import org.apache.samoa.moa.evaluation.LearningEvaluation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class EvaluatorCVProcessor implements Processor {

  /**
   *
   */
  private static final long serialVersionUID = -2778051819116753612L;

  private static final Logger logger = LoggerFactory.getLogger(EvaluatorCVProcessor.class);

  private static final String ORDERING_MEASUREMENT_NAME = "evaluation instances";

  private final PerformanceEvaluator[] evaluators;
  private final int samplingFrequency;
  private final File dumpFile;
  private transient PrintStream immediateResultStream = null;
  private transient boolean firstDump = true;

  private long totalCount = 0;
  private long experimentStart = 0;

  private long sampleStart = 0;

  private LearningCurve learningCurve;
  private int id;

  private int foldNumber = 10;

  private EvaluatorCVProcessor(Builder builder) {
    evaluators = new PerformanceEvaluator[builder.foldNumber];
    for (int i = 0; i < this.evaluators.length; i++) {
      evaluators[i] = (PerformanceEvaluator) builder.evaluator.copy();
    }
    this.samplingFrequency = builder.samplingFrequency;
    this.dumpFile = builder.dumpFile;
    this.foldNumber = builder.foldNumber;
  }

  private boolean initiated = false;

  @Override
  public boolean process(ContentEvent event) {

    if (this.initiated == false) {
      sampleStart = System.nanoTime();
      experimentStart = sampleStart;
      this.initiated = true;
    }

    ResultContentEvent result = (ResultContentEvent) event;
    int instanceIndex = (int) result.getInstanceIndex();

    addStatisticsForInstanceReceived(instanceIndex, result.getEvaluationIndex(), 1);

    evaluators[result.getEvaluationIndex()].addResult(result.getInstance(), result.getClassVotes(),
        String.valueOf(instanceIndex), result.getArrivalTimestamp());

    if (hasAllVotesArrivedInstance(instanceIndex)) {
      totalCount += 1;
      if (result.isLastEvent()) {
        this.concludeMeasurement();
        return true;
      }
      //this.mapCountsforInstanceReceived.remove(instanceIndex);

      if ((totalCount > 0) && (totalCount % samplingFrequency) == 0) {
        long sampleEnd = System.nanoTime();
        long sampleDuration = TimeUnit.SECONDS.convert(sampleEnd - sampleStart, TimeUnit.NANOSECONDS);
        sampleStart = sampleEnd;

        logger.info("{} seconds for {} instances", sampleDuration, samplingFrequency);
        this.addMeasurement();
      }
    }

    return false;
  }

  protected Map<Integer, Integer> mapCountsforInstanceReceived;

  private boolean hasAllVotesArrivedInstance(int instanceIndex) {
    Map<Integer, Integer> map = this.mapCountsforInstanceReceived;
    int count = map.get(instanceIndex);
    return (count == this.foldNumber);
  }

  protected void addStatisticsForInstanceReceived(int instanceIndex, int evaluationIndex, int add) {
    if (this.mapCountsforInstanceReceived == null) {
      this.mapCountsforInstanceReceived = new HashMap<>();
    }
    Integer count = this.mapCountsforInstanceReceived.get(instanceIndex);
    if (count == null) {
      count = 0;
    }
    this.mapCountsforInstanceReceived.put(instanceIndex, count + add);
  }

  @Override
  public void onCreate(int id) {
    this.id = id;
    this.learningCurve = new LearningCurve(ORDERING_MEASUREMENT_NAME);

    if (this.dumpFile != null) {
      try {
        if (dumpFile.exists()) {
          this.immediateResultStream = new PrintStream(
              new FileOutputStream(dumpFile, true), true);
        } else {
          this.immediateResultStream = new PrintStream(
              new FileOutputStream(dumpFile), true);
        }

      } catch (FileNotFoundException e) {
        this.immediateResultStream = null;
        logger.error("File not found exception for {}:{}", this.dumpFile.getAbsolutePath(), e.toString());

      } catch (Exception e) {
        this.immediateResultStream = null;
        logger.error("Exception when creating {}:{}", this.dumpFile.getAbsolutePath(), e.toString());
      }
    }

    this.firstDump = true;
  }

  @Override
  public Processor newProcessor(Processor p) {
    EvaluatorCVProcessor originalProcessor = (EvaluatorCVProcessor) p;
    EvaluatorCVProcessor newProcessor = new EvaluatorCVProcessor.Builder(originalProcessor).build();

    if (originalProcessor.learningCurve != null) {
      newProcessor.learningCurve = originalProcessor.learningCurve;
    }

    return newProcessor;
  }

  @Override
  public String toString() {
    StringBuilder report = new StringBuilder();

    report.append(EvaluatorCVProcessor.class.getCanonicalName());
    report.append("id = ").append(this.id);
    report.append('\n');

    if (learningCurve.numEntries() > 0) {
      report.append(learningCurve.toString());
      report.append('\n');
    }
    return report.toString();
  }

  private void addMeasurement() {
    List<Measurement> measurements = new Vector<>();
    measurements.add(new Measurement(ORDERING_MEASUREMENT_NAME, totalCount));

    Measurement[] finalMeasurements = getEvaluationMeasurements(
        measurements.toArray(new Measurement[measurements.size()]), evaluators);

    LearningEvaluation learningEvaluation = new LearningEvaluation(finalMeasurements);
    learningCurve.insertEntry(learningEvaluation);
    logger.debug("evaluator id = {}", this.id);
    logger.info(learningEvaluation.toString());

    if (immediateResultStream != null) {
      if (firstDump) {
        immediateResultStream.println(learningCurve.headerToString());
        firstDump = false;
      }

      immediateResultStream.println(learningCurve.entryToString(learningCurve.numEntries() - 1));
      immediateResultStream.flush();
    }
  }

  private void concludeMeasurement() {
    logger.info("last event is received!");
    logger.info("total count: {}", this.totalCount);

    String learningCurveSummary = this.toString();
    logger.info(learningCurveSummary);

    long experimentEnd = System.nanoTime();
    long totalExperimentTime = TimeUnit.SECONDS.convert(experimentEnd - experimentStart, TimeUnit.NANOSECONDS);
    logger.info("total evaluation time: {} seconds for {} instances", totalExperimentTime, totalCount);

    if (immediateResultStream != null) {
      immediateResultStream.println("# COMPLETED");
      immediateResultStream.flush();
    }
    // logger.info("average throughput rate: {} instances/seconds",
    // (totalCount/totalExperimentTime));
  }

  public static class Builder {

    private final PerformanceEvaluator evaluator;
    private int samplingFrequency = 100000;
    private File dumpFile = null;
    private int foldNumber = 10;

    public Builder(PerformanceEvaluator evaluator) {
      this.evaluator = evaluator;
    }

    public Builder(EvaluatorCVProcessor oldProcessor) {
      this.evaluator = oldProcessor.evaluators[0];
      this.samplingFrequency = oldProcessor.samplingFrequency;
      this.dumpFile = oldProcessor.dumpFile;
    }

    public Builder samplingFrequency(int samplingFrequency) {
      this.samplingFrequency = samplingFrequency;
      return this;
    }

    public Builder dumpFile(File file) {
      this.dumpFile = file;
      return this;
    }

    public Builder foldNumber(int foldNumber) {
      this.foldNumber = foldNumber;
      return this;
    }

    public EvaluatorCVProcessor build() {
      return new EvaluatorCVProcessor(this);
    }
  }

  public Measurement[] getEvaluationMeasurements(Measurement[] modelMeasurements,
      PerformanceEvaluator[] subEvaluators) {
    List<Measurement> measurementList = new LinkedList<Measurement>();
    if (modelMeasurements != null) {
      measurementList.addAll(Arrays.asList(modelMeasurements));
    }
    // add average of sub-model measurements
    if ((subEvaluators != null) && (subEvaluators.length > 0)) {
      List<Measurement[]> subMeasurements = new LinkedList<Measurement[]>();
      for (PerformanceEvaluator subEvaluator : subEvaluators) {
        if (subEvaluator != null) {
          subMeasurements.add(subEvaluator.getPerformanceMeasurements());
        }
      }
      Measurement[] avgMeasurements = Measurement
          .averageMeasurements(subMeasurements.toArray(new Measurement[subMeasurements.size()][]));
      measurementList.addAll(Arrays.asList(avgMeasurements));
    }
    return measurementList.toArray(new Measurement[measurementList.size()]);
  }
}
