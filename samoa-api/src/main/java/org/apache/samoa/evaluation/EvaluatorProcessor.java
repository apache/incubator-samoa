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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Attribute;
import org.apache.samoa.instances.Utils;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.evaluation.LearningCurve;
import org.apache.samoa.moa.evaluation.LearningEvaluation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

public class EvaluatorProcessor implements Processor {

  /**
	 * 
	 */
  private static final long serialVersionUID = -2778051819116753612L;

  private static final Logger logger =
      LoggerFactory.getLogger(EvaluatorProcessor.class);

  private static final String ORDERING_MEASUREMENT_NAME = "evaluation instances";

  private final PerformanceEvaluator evaluator;
  private final int samplingFrequency;
  private final File dumpFile;
  private final File resultFile;
  private transient PrintStream immediateResultStream = null;
  private transient PrintStream immediateOutputStream = null;
  private transient boolean firstDump = true;
  private transient boolean firstResult = true;  

  private long totalCount = 0;
  private long experimentStart = 0;

  private long sampleStart = 0;

  private LearningCurve learningCurve;
  private int id;

  private EvaluatorProcessor(Builder builder) {
    this.evaluator = builder.evaluator;
    this.samplingFrequency = builder.samplingFrequency;
    this.dumpFile = builder.dumpFile;
    this.resultFile = builder.resultFile;
  }

  @Override
  public boolean process(ContentEvent event) {

    ResultContentEvent result = (ResultContentEvent) event;

    if ((totalCount > 0) && (totalCount % samplingFrequency) == 0) {
      long sampleEnd = System.nanoTime();
      long sampleDuration = TimeUnit.SECONDS.convert(sampleEnd - sampleStart, TimeUnit.NANOSECONDS);
      sampleStart = sampleEnd;

      logger.info("{} seconds for {} instances", sampleDuration, samplingFrequency);
      this.addMeasurement();
    }
    
    if (immediateOutputStream != null) {
      String instanceIndex = String.valueOf(result.getInstanceIndex());
      Attribute classAttribute = result.getInstance().dataset().classAttribute();
      int numClasses = result.getInstance().dataset().numClasses();
      double trueValue = result.getInstance().classValue();
      //for classification
      if (classAttribute.isNominal()) {
        List<String> classAttributeValues = classAttribute.getAttributeValues();
        if (this.firstResult) {
          //immediateOutputStream.println("Class votes");
          String classHeader = "";
          for (String value : classAttributeValues) {
            classHeader += "votes_" + value + ",";
          }
          //immediateOutputStream.println("instance number,true class value,predicted class value," + "votes:" + classAttributeValues.toString().replaceAll("\\s+",""));
          immediateOutputStream.println("instance number,true class value,predicted class value," + classHeader.substring(0, classHeader.length() - 1));
          this.firstResult=false;
        }        
        int trueNominalIndex = (int) trueValue;
        String trueNominalValue = classAttributeValues.get(trueNominalIndex);
        double[] votes =  Arrays.copyOf(result.getClassVotes(), numClasses);
        //votes = votes.substring(1, votes.length() - 1);      
        String predictedNominalValue = classAttributeValues.get(Utils.maxIndex(votes));
        immediateOutputStream.println(instanceIndex + "," + trueNominalValue + "," + predictedNominalValue + "," + Arrays.toString(votes).replaceAll("\\s+","").replaceAll("\\[", "").replaceAll("\\]",""));
        immediateOutputStream.flush();
      }
      //for regression
      if (classAttribute.isNumeric()) {
        if (this.firstResult) {
          immediateOutputStream.println("instance_number,true_value,predicted_value");
          this.firstResult=false;
        }
        //logger.info("result: " + Arrays.toString(result.getClassVotes()));
        double predictedValue = 0;
        if (result.getClassVotes().length > 0)
          predictedValue = result.getClassVotes()[0];
        immediateOutputStream.println(instanceIndex + "," + trueValue + "," + predictedValue);
      }      
    }

    if (result.isLastEvent()) {
      this.concludeMeasurement();
      return true;
    }

    evaluator.addResult(result.getInstance(), result.getClassVotes());
    totalCount += 1;

    if (totalCount == 1) {
      sampleStart = System.nanoTime();
      experimentStart = sampleStart;
    }

    return false;
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
    
    if (this.resultFile != null) {
      try {
        this.immediateOutputStream = new PrintStream(new FileOutputStream(resultFile), true);
      } catch (FileNotFoundException e) {
        this.immediateOutputStream = null;
        logger.error("File not found exception for {}:{}", this.resultFile.getAbsolutePath(), e.toString());
      } catch (Exception e) {
        this.immediateOutputStream = null;
        logger.error("Exception when creating {}:{}", this.resultFile.getAbsolutePath(), e.toString());
      }
    }

    this.firstDump = true;
  }

  @Override
  public Processor newProcessor(Processor p) {
    EvaluatorProcessor originalProcessor = (EvaluatorProcessor) p;
    EvaluatorProcessor newProcessor = new EvaluatorProcessor.Builder(originalProcessor).build();

    if (originalProcessor.learningCurve != null) {
      newProcessor.learningCurve = originalProcessor.learningCurve;
    }

    return newProcessor;
  }

  @Override
  public String toString() {
    StringBuilder report = new StringBuilder();

    report.append(EvaluatorProcessor.class.getCanonicalName());
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

    Collections.addAll(measurements, evaluator.getPerformanceMeasurements());

    Measurement[] finalMeasurements = measurements.toArray(new Measurement[measurements.size()]);

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
      //
      immediateResultStream.println("# Total evaluation time: " + totalExperimentTime + " seconds for "  + totalCount + " instances");
      immediateResultStream.flush();
    }
    // logger.info("average throughput rate: {} instances/seconds",
    // (totalCount/totalExperimentTime));
  }

  public static class Builder {

    private final PerformanceEvaluator evaluator;
    private int samplingFrequency = 100000;
    private File dumpFile = null;
    private File resultFile = null;

    public Builder(PerformanceEvaluator evaluator) {
      this.evaluator = evaluator;
    }

    public Builder(EvaluatorProcessor oldProcessor) {
      this.evaluator = oldProcessor.evaluator;
      this.samplingFrequency = oldProcessor.samplingFrequency;
      this.dumpFile = oldProcessor.dumpFile;
      this.resultFile = oldProcessor.resultFile;
    }

    public Builder samplingFrequency(int samplingFrequency) {
      this.samplingFrequency = samplingFrequency;
      return this;
    }

    public Builder dumpFile(File file) {
      this.dumpFile = file;
      return this;
    }
    
    public Builder resultFile(File file) {
      this.resultFile = file;
      return this;
    }

    public EvaluatorProcessor build() {
      return new EvaluatorProcessor(this);
    }
  }
}
