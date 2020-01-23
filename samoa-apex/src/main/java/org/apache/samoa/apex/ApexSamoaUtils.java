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

package org.apache.samoa.apex;

import com.github.javacliparser.ClassOption;

import org.apache.samoa.apex.topology.impl.ApexComponentFactory;
import org.apache.samoa.apex.topology.impl.ApexTopology;
import org.apache.samoa.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApexSamoaUtils {

  private static final Logger logger = LoggerFactory.getLogger(ApexSamoaUtils.class);

  public static ApexTopology argsToTopology(String[] args) {
    StringBuilder cliString = new StringBuilder();
    for (String arg : args) {
      cliString.append(" ").append(arg);
    }
    logger.debug("Command line string = {}", cliString.toString());

    Task task = getTask(cliString.toString());

    // TODO: remove setFactory method with DynamicBinding
    task.setFactory(new ApexComponentFactory());
    task.init();

    return (ApexTopology) task.getTopology();
  }

  public static Task getTask(String cliString) {
    Task task = null;
    try {
      logger.debug("Providing task [{}]", cliString);
      task = ClassOption.cliStringToObject(cliString, Task.class, null);
    } catch (Exception e) {
      logger.warn("Fail in initializing the task!");
      e.printStackTrace();
    }
    return task;
  }
}
