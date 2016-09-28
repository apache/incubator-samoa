package org.apache.samoa.topology.impl.gearpump;

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

import com.github.javacliparser.ClassOption;

import org.apache.samoa.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Utils {

  private static final Logger logger = LoggerFactory.getLogger(Utils.class);
  public static String piConf = "processingItem";
  public static String entrancePiConf = "entranceProcessingItem";

  public static byte[] objectToBytes(Object object) {
    ObjectOutputStream oos;
    ByteArrayOutputStream baos;
    try {
      baos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
      oos.flush();
      byte[] bytes = baos.toByteArray();
      baos.close();
      oos.close();
      return bytes;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public static Object bytesToObject(byte[] bytes) {
    ByteArrayInputStream bais;
    ObjectInputStream ois;
    try {
      bais = new ByteArrayInputStream(bytes);
      ois = new ObjectInputStream(bais);
      Object object = ois.readObject();
      bais.close();
      ois.close();
      return object;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static GearpumpTopology argsToTopology(String[] args) {
    StringBuilder cliString = new StringBuilder();
    for (String arg : args) {
      cliString.append(" ").append(arg);
    }
    logger.info("Command line string = {}", cliString.toString());

    Task task = getTask(cliString.toString());

    // TODO: remove setFactory method with DynamicBinding
    task.setFactory(new ComponentFactory());
    task.init();

    return (GearpumpTopology) task.getTopology();
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
