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

package org.apache.samoa.streams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.samoa.instances.Instances;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.tasks.TaskMonitor;

import com.github.javacliparser.FileOption;
import com.github.javacliparser.IntOption;

/**
 * InstanceStream for ARFF file
 * 
 */
public class ArffFileStream extends FileStream {

  public FileOption arffFileOption = new FileOption("arffFile", 'f',
      "ARFF File(s) to load.", null, null, false);

  /*public IntOption classIndexOption = new IntOption("classIndex", 'c',
      "Class index of data. 0 for none or -1 for last attribute in file.",
      -1, -1, Integer.MAX_VALUE);*/

  protected InstanceExample lastInstanceRead;
  private transient BufferedReader fileReader;

  @Override
  public void prepareForUseImpl(TaskMonitor monitor, ObjectRepository repository) {
    super.prepareForUseImpl(monitor, repository);
    String filePath = this.arffFileOption.getFile().getAbsolutePath();
    this.fileSource.init(filePath, "arff");
    this.lastInstanceRead = null;
  }

  @Override
  protected void reset() {
    try {
      fileSource.reset();
    } catch (IOException ioe) {
      throw new RuntimeException("FileStream restart failed.", ioe);
    }

    if (!getNextFileStream()) {
      hitEndOfStream = true;
      throw new RuntimeException("FileStream is empty.");
    }
  }

  @Override
  protected boolean getNextFileStream() {
    if (this.fileReader != null)
      try {
        this.fileReader.close();
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }

    this.inputStream = this.fileSource.getNextInputStream();
    if (inputStream == null)
      return false;

    this.fileReader = new BufferedReader(new InputStreamReader(this.inputStream));
    this.instances = new Instances(this.fileReader, 1, -1);
    if (this.classIndexOption.getValue() < 0) {
      this.instances.setClassIndex(this.instances.numAttributes() - 1);
    } else if (this.classIndexOption.getValue() > 0) {
      this.instances.setClassIndex(this.classIndexOption.getValue() - 1);
    }

    return true;
  }

  @Override
  protected boolean readNextInstanceFromFile() {
    try {
      if (this.instances.readInstance(this.fileReader)) {
        this.lastInstanceRead = new InstanceExample(this.instances.instance(0));
        this.instances.delete(); // keep instances clean
        return true;
      }
      if (this.fileReader != null) {
        this.fileReader.close();
        this.fileReader = null;
      }
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException(
          "ArffFileStream failed to read instance from stream.", ioe);
    }

  }

  @Override
  protected InstanceExample getLastInstanceRead() {
    return this.lastInstanceRead;
  }

  /*
   * extend org.apache.samoa.moa.MOAObject
   */
  @Override
  public void getDescription(StringBuilder sb, int indent) {
    // TODO Auto-generated method stub
  }
}
