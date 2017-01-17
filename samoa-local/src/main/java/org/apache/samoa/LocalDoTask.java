package org.apache.samoa;

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

import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.impl.SimpleComponentFactory;
import org.apache.samoa.topology.impl.SimpleEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.Option;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

/**
 * The Class DoTask.
 */
public class LocalDoTask {

  // TODO: clean up this class for helping ML Developer in SAMOA
  // TODO: clean up code from storm-impl

  // It seems that the 3 extra options are not used.
  // Probably should remove them
  private static final String SUPPRESS_STATUS_OUT_MSG = "Suppress the task status output. Normally it is sent to stderr.";
  private static final String SUPPRESS_RESULT_OUT_MSG = "Suppress the task result output. Normally it is sent to stdout.";
  private static final String STATUS_UPDATE_FREQ_MSG = "Wait time in milliseconds between status updates.";
  private static final Logger logger = LoggerFactory.getLogger(LocalDoTask.class);

  public static String command = "";
  public static String dataSet = "";
  private static transient File metrics;
  private static transient PrintStream metadataStream = null;
//
  /**
   * The main method.
   * 
   * @param args
   *          the arguments
   */
  public static void main(String[] args) {

    // ArrayList<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));

    // args = tmpArgs.toArray(new String[0]);

    FlagOption suppressStatusOutOpt = new FlagOption("suppressStatusOut", 'S', SUPPRESS_STATUS_OUT_MSG);

    FlagOption suppressResultOutOpt = new FlagOption("suppressResultOut", 'R', SUPPRESS_RESULT_OUT_MSG);

    IntOption statusUpdateFreqOpt = new IntOption("statusUpdateFrequency", 'F', STATUS_UPDATE_FREQ_MSG, 1000, 0,
        Integer.MAX_VALUE);

    Option[] extraOptions = new Option[] { suppressStatusOutOpt, suppressResultOutOpt, statusUpdateFreqOpt };

    StringBuilder cliString = new StringBuilder();
    for (String arg : args) {
      if (arg.endsWith(".arff)")) {
        dataSet = (arg.substring(arg.lastIndexOf("/") + 1));
        dataSet = dataSet.substring(0, dataSet.length()-1);
      }
      cliString.append(" ").append(arg);
    }
    logger.debug("Command line string = {}", cliString.toString());
    command = cliString.toString();
    System.out.println("Command line string = " + cliString.toString());
    try {
      String datapath = "/Users/fobeligi/Documents/GBDT/experiments-output-310317/forestCoverType/";
      File metrics = new File(datapath+dataSet+"_commands.csv");
      metadataStream = new PrintStream(
              new FileOutputStream(metrics), true);
      metadataStream.println("Command,dataset,framework,Experiment duration" + command);
      metadataStream.print(command + ","+ dataSet+ ",LOCAL");
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    
    
    Task task;
    try {
      task = ClassOption.cliStringToObject(cliString.toString(), Task.class, extraOptions);
      logger.info("Successfully instantiating {}", task.getClass().getCanonicalName());
    } catch (Exception e) {
      logger.error("Fail to initialize the task", e);
      System.out.println("Fail to initialize the task" + e);
      return;
    }
    task.setFactory(new SimpleComponentFactory());
    
    task.init();
    SimpleEngine.submitTopology(task.getTopology());
  }
}
