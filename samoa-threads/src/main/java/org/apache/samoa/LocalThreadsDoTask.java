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

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.impl.ThreadsComponentFactory;
import org.apache.samoa.topology.impl.ThreadsEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javacliparser.ClassOption;

/**
 * @author Anh Thu Vu
 * 
 */
public class LocalThreadsDoTask {
  private static final Logger logger = LoggerFactory.getLogger(LocalThreadsDoTask.class);

  /**
   * The main method.
   * 
   * @param args
   *          the arguments
   */
  public static void main(String[] args) {

    ArrayList<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));

    // Get number of threads for multithreading mode
    int numThreads = 1;
    for (int i = 0; i < tmpArgs.size() - 1; i++) {
      if (tmpArgs.get(i).equals("-t")) {
        try {
          numThreads = Integer.parseInt(tmpArgs.get(i + 1));
          tmpArgs.remove(i + 1);
          tmpArgs.remove(i);
        } catch (NumberFormatException e) {
          System.err.println("Invalid number of threads.");
          System.err.println(e.getStackTrace());
        }
      }
    }
    logger.info("Number of threads:{}", numThreads);

    args = tmpArgs.toArray(new String[0]);

    StringBuilder cliString = new StringBuilder();
    for (int i = 0; i < args.length; i++) {
      cliString.append(" ").append(args[i]);
    }
    logger.debug("Command line string = {}", cliString.toString());
    System.out.println("Command line string = " + cliString.toString());

    Task task = null;
    try {
      task = (Task) ClassOption.cliStringToObject(cliString.toString(), Task.class, null);
      logger.info("Sucessfully instantiating {}", task.getClass().getCanonicalName());
    } catch (Exception e) {
      logger.error("Fail to initialize the task", e);
      System.out.println("Fail to initialize the task" + e);
      return;
    }
    task.setFactory(new ThreadsComponentFactory());
    task.init();

    ThreadsEngine.submitTopology(task.getTopology(), numThreads);
  }
}
