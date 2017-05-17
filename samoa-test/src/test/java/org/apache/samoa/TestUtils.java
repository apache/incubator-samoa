package org.apache.samoa;/*
* #%L
 * * SAMOA
 * *
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
 * *
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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class.getName());

  public static void test(final TestParams testParams) throws IOException, ClassNotFoundException,
      NoSuchMethodException, InvocationTargetException, IllegalAccessException, InterruptedException {

    final File tempFile = File.createTempFile("test", "test");
    final File labelFile = File.createTempFile("result", "result");
    //final File labelFile = null;
    LOG.info("Starting test, output file is {}, test config is \n{}", tempFile.getAbsolutePath(), testParams.toString());  
    Executors.newSingleThreadExecutor().submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {          
          Class.forName(testParams.getTaskClassName())
                .getMethod("main", String[].class)
                .invoke(null, (Object) String.format(
                    testParams.getCliStringTemplate(),
                    tempFile.getAbsolutePath(),
                    testParams.getInputInstances(),
                    testParams.getSamplingSize(),
                    testParams.getInputDelayMicroSec(),
                    labelFile.getAbsolutePath()
                    ).split("[ ]"));
          } catch (Exception e) {
            LOG.error("Cannot execute test {} {}", e.getMessage(), e.getCause().getMessage());
          }
          return null;
        }
      });

    Thread.sleep(TimeUnit.SECONDS.toMillis(testParams.getPrePollWaitSeconds()));

    CountDownLatch signalComplete = new CountDownLatch(1);

    final Tailer tailer = Tailer.create(tempFile, new TestResultsTailerAdapter(signalComplete), 1000);
    new Thread(new Runnable() {
      @Override
      public void run() {
        tailer.run();
      }
    }).start();

    signalComplete.await();
    tailer.stop();

    assertResults(tempFile, testParams);
    if (testParams.getLabelFileCreated())
      assertLabels(labelFile, testParams);
  }

  public static void assertResults(File outputFile, org.apache.samoa.TestParams testParams) throws IOException {

    LOG.info("Checking results file " + outputFile.getAbsolutePath());
    // 1. parse result file with csv parser
    Reader in = new FileReader(outputFile);
    Iterable<CSVRecord> records = CSVFormat.EXCEL.withSkipHeaderRecord(false)
        .withIgnoreEmptyLines(true).withDelimiter(',').withCommentMarker('#').parse(in);
    CSVRecord last = null;
    Iterator<CSVRecord> iterator = records.iterator();
    CSVRecord header = iterator.next();
    // Results of Standard Evaluation have 5 columns, and cv Evaluation have 9 columns
    int cvEvaluation = (header.size() == 9) ? 1: 0;
    String cvText = (header.size() == 9) ? "[avg] " : "";

    Assert
        .assertEquals("Unexpected column", org.apache.samoa.TestParams.EVALUATION_INSTANCES, header.get(0).trim());
    Assert
        .assertEquals("Unexpected column", cvText + org.apache.samoa.TestParams.CLASSIFIED_INSTANCES, header.get(1).trim());
    Assert.assertEquals("Unexpected column", cvText + org.apache.samoa.TestParams.CLASSIFICATIONS_CORRECT, header.get(2 + cvEvaluation)
        .trim());
    Assert.assertEquals("Unexpected column", cvText + org.apache.samoa.TestParams.KAPPA_STAT, header.get(3 + 2 * cvEvaluation).trim());
    Assert.assertEquals("Unexpected column", cvText + org.apache.samoa.TestParams.KAPPA_TEMP_STAT, header.get(4 + 3 * cvEvaluation).trim());

    // 2. check last line result
    while (iterator.hasNext()) {
      last = iterator.next();
    }

    assertTrue(String.format("Unmet threshold expected %d got %f",
        testParams.getEvaluationInstances(), Float.parseFloat(last.get(0))),
        testParams.getEvaluationInstances() <= Float.parseFloat(last.get(0)));
    assertTrue(String.format("Unmet threshold expected %d got %f", testParams.getClassifiedInstances(),
        Float.parseFloat(last.get(1))),
        testParams.getClassifiedInstances() <= Float.parseFloat(last.get(1)));
    assertTrue(String.format("Unmet threshold expected %f got %f",
        testParams.getClassificationsCorrect(), Float.parseFloat(last.get(2 + cvEvaluation))),
        testParams.getClassificationsCorrect() <= Float.parseFloat(last.get(2 + cvEvaluation)));
    assertTrue(String.format("Unmet threshold expected %f got %f",
        testParams.getKappaStat(), Float.parseFloat(last.get(3 + 2 * cvEvaluation))),
        testParams.getKappaStat() <= Float.parseFloat(last.get(3 + 2 * cvEvaluation)));
    assertTrue(String.format("Unmet threshold expected %f got %f",
        testParams.getKappaTempStat(), Float.parseFloat(last.get(4 + 3 * cvEvaluation))),
        testParams.getKappaTempStat() <= Float.parseFloat(last.get(4 + 3 * cvEvaluation)));

  }
  
  public static void assertLabels(File labelFile, org.apache.samoa.TestParams testParams) throws IOException {
    LOG.info("Checking labels file " + labelFile.getAbsolutePath());
    //1. parse result file with csv parser
    Reader in = new FileReader(labelFile);
    Iterable<CSVRecord> records = CSVFormat.EXCEL.withSkipHeaderRecord(false)
        .withIgnoreEmptyLines(true).withDelimiter(',').withCommentMarker('#').parse(in);
    //CSVRecord last = null;
    Iterator<CSVRecord> iterator = records.iterator();
    CSVRecord header = iterator.next();
    long instanceCount = 0;

    Assert.assertEquals("Unexpected column", org.apache.samoa.TestParams.INSTANCE_ID, header.get(0).trim());
    Assert.assertEquals("Unexpected column", org.apache.samoa.TestParams.TRUE_CLASS_VALUE, header.get(1).trim());
    Assert.assertEquals("Unexpected column", org.apache.samoa.TestParams.PREDICTED_CLASS_VALUE, header.get(2).trim());
    for (int i = 3; i < header.size(); i++)
      Assert.assertEquals("Unexpected column", org.apache.samoa.TestParams.VOTES, header.get(i).trim().substring(0, org.apache.samoa.TestParams.VOTES.length()));

    // 2. check last line result
    /*while (iterator.hasNext()) {
      instanceCount++;
      //last = iterator.next();
    }*/

    /*assertTrue(String.format("Unmet threshold expected %d got %f",
        testParams.getEvaluationInstances(), instanceCount),
        instanceCount <= testParams.getEvaluationInstances());*/
  }

  private static class TestResultsTailerAdapter extends TailerListenerAdapter {

    private final CountDownLatch signalComplete;

    public TestResultsTailerAdapter(CountDownLatch signalComplete) {
      this.signalComplete = signalComplete;
    }

    @Override
    public void handle(String line) {
      if ("# COMPLETED".equals(line.trim())) {
        signalComplete.countDown();
      }
    }
  }

}
