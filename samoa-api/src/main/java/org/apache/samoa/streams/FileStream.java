package org.apache.samoa.streams;

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
import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.ListOption;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.fs.FileStreamSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * InstanceStream for files (Abstract class: subclass this class for different file formats)
 * 
 * @author Casey
 */
public abstract class FileStream extends AbstractOptionHandler implements InstanceStream {
  /**
    *
    */
  private static final long serialVersionUID = 3028905554604259130L;

  public ClassOption sourceTypeOption = new ClassOption("sourceType",
      's', "Source Type (HDFS, local FS)", FileStreamSource.class,
      "LocalFileStreamSource");

  public IntOption classIndexOption = new IntOption("classIndex", 'c',
          "Class index of data. 0 for none or -1 for last attribute in file.", -1, -1, Integer.MAX_VALUE);

  private FloatOption floatOption = new FloatOption("classWeight", 'w', "", 1.0);
  public ListOption classWeightsOption = new ListOption("classWeights", 'w',
          "Class weights in order of class index.", floatOption, new FloatOption[0], ':');

  protected transient FileStreamSource fileSource;
  //protected transient Reader fileReader;
  protected transient InputStream inputStream;
  protected Instances instances;
  protected FloatOption[] classWeights; // = new FloatOption[0];

  protected boolean hitEndOfStream;
  private boolean hasStarted;

  /*
   * Constructors
   */
  public FileStream() {
    this.hitEndOfStream = false;
  }

  /*
   * implement InstanceStream
   */
  @Override
  public InstancesHeader getHeader() {
    return new InstancesHeader(this.instances);
  }

  @Override
  public long estimatedRemainingInstances() {
    return -1;
  }

  @Override
  public boolean hasMoreInstances() {
    if (this.hitEndOfStream) {
      if (getNextFileStream()) {
        this.hitEndOfStream = false;
        return hasMoreInstances();
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  @Override
  public InstanceExample nextInstance() {
    if (this.getLastInstanceRead() == null) {
      readNextInstanceFromStream();
    }
    InstanceExample prevInstance = this.getLastInstanceRead();
    if (classWeights != null && classWeights.length > 0) {
      int i = (int) prevInstance.instance.classValue();
      double w = 1.0;
      if (i >= 0 && i < classWeights.length)
        w = classWeights[i].getValue();
      prevInstance.setWeight(w);
    }
    readNextInstanceFromStream();
    return prevInstance;
  }

  @Override
  public boolean isRestartable() {
    return true;
  }

  @Override
  public void restart() {
    reset();
    hasStarted = false;
  }

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

  protected abstract boolean getNextFileStream();

  protected boolean readNextInstanceFromStream() {
    if (!hasStarted) {
      this.reset();
      hasStarted = true;
    }

    while (true) {
      if (readNextInstanceFromFile())
        return true;

      if (!getNextFileStream()) {
        this.hitEndOfStream = true;
        return false;
      }
    }
  }

  /**
   * Read next instance from the current file and assign it to lastInstanceRead.
   * 
   * @return true if it was able to read next instance and false if it was at the end of the file
   */
  protected abstract boolean readNextInstanceFromFile();

  protected abstract InstanceExample getLastInstanceRead();

  @Override
  public void prepareForUseImpl(TaskMonitor monitor, ObjectRepository repository) {
    this.fileSource = sourceTypeOption.getValue();
    this.classWeights = Arrays.copyOf(classWeightsOption.getList(), classWeightsOption.getList().length, FloatOption[].class);
    this.hasStarted = false;
  }

}
