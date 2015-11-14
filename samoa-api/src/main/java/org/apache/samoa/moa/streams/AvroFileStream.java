package org.apache.samoa.moa.streams;

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

import java.io.IOException;
import java.io.InputStream;

import org.apache.samoa.instances.Instances;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.FileStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javacliparser.FileOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;

/**
 * InstanceStream implementation to handle Apache Avro Files. Handles both JSON & Binary encoded streams
 * 
 *
 */
public class AvroFileStream extends FileStream {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory.getLogger(AvroFileStream.class);

  public FileOption avroFileOption = new FileOption("avroFile", 'f', "Avro File(s) to load.", null, null, false);
  public IntOption classIndexOption = new IntOption("classIndex", 'c',
      "Class index of data. 0 for none or -1 for last attribute in file.", -1, -1, Integer.MAX_VALUE);
  public StringOption encodingFormatOption = new StringOption("encodingFormatOption", 'e',
      "Encoding format for Avro Files. Can be JSON/AVRO", "BINARY");

  /** Represents the last read Instance **/
  protected InstanceExample lastInstanceRead;

  /** Represents the binary input stream of avro data **/
  protected transient InputStream inputStream = null;

  /** The extension to be considered for the files **/
  private static final String AVRO_FILE_EXTENSION = "avro";

  /* (non-Javadoc)
   * @see org.apache.samoa.streams.FileStream#reset()
   * Reset the BINARY encoded Avro Stream & Close the file source
   */
  @Override
  protected void reset() {

    try {
      if (this.inputStream != null)
        this.inputStream.close();

      fileSource.reset();
    } catch (IOException ioException) {
      logger.error(AVRO_STREAM_FAILED_RESTART_ERROR + " : {}", ioException);
      throw new RuntimeException(AVRO_STREAM_FAILED_RESTART_ERROR, ioException);
    }

    if (!getNextFileStream()) {
      hitEndOfStream = true;
      throw new RuntimeException(AVRO_STREAM_EMPTY_STREAM_ERROR);
    }
  }

  /**
   * Get next File Stream & set the class index read from the command line option
   * 
   * @return
   */
  protected boolean getNextFileStream() {
    if (this.inputStream != null)
      try {
        this.inputStream.close();
      } catch (IOException ioException) {
        logger.error(AVRO_STREAM_FAILED_READ_ERROR + " : {}", ioException);
        throw new RuntimeException(AVRO_STREAM_FAILED_READ_ERROR, ioException);
      }

    this.inputStream = this.fileSource.getNextInputStream();

    if (this.inputStream == null)
      return false;

    this.instances = new Instances(this.inputStream, classIndexOption.getValue(), encodingFormatOption.getValue());

    if (this.classIndexOption.getValue() < 0) {
      this.instances.setClassIndex(this.instances.numAttributes() - 1);
    } else if (this.classIndexOption.getValue() > 0) {
      this.instances.setClassIndex(this.classIndexOption.getValue() - 1);
    }
    return true;
  }

  /* (non-Javadoc)
   * @see org.apache.samoa.streams.FileStream#readNextInstanceFromFile()
   * Read next Instance from File. Return false if unable to read next Instance
   */
  @Override
  protected boolean readNextInstanceFromFile() {
    try {
      if (this.instances.readInstance()) {
        this.lastInstanceRead = new InstanceExample(this.instances.instance(0));
        this.instances.delete();
        return true;
      }
      if (this.inputStream != null) {
        this.inputStream.close();
        this.inputStream = null;
      }
      return false;
    } catch (IOException ioException) {
      logger.error(AVRO_STREAM_FAILED_READ_ERROR + " : {}", ioException);
      throw new RuntimeException(AVRO_STREAM_FAILED_READ_ERROR, ioException);
    }

  }

  @Override
  public void prepareForUseImpl(TaskMonitor monitor, ObjectRepository repository) {
    super.prepareForUseImpl(monitor, repository);
    String filePath = this.avroFileOption.getFile().getAbsolutePath();
    this.fileSource.init(filePath, AvroFileStream.AVRO_FILE_EXTENSION);
    this.lastInstanceRead = null;
  }

  /* (non-Javadoc)
   * @see org.apache.samoa.streams.FileStream#getLastInstanceRead()
   * Return the last read Instance
   */
  @Override
  protected InstanceExample getLastInstanceRead() {
    return this.lastInstanceRead;
  }

  @Override
  public void getDescription(StringBuilder sb, int indent) {
    throw new UnsupportedOperationException(AVRO_STREAM_UNSUPPORTED_METHOD);
  }

  /** Error Messages to for all types of Avro File Streams */
  protected static final String AVRO_STREAM_FAILED_RESTART_ERROR = "Avro FileStream restart failed.";
  protected static final String AVRO_STREAM_EMPTY_STREAM_ERROR = "Avro FileStream is empty.";
  protected static final String AVRO_STREAM_FAILED_READ_ERROR = "Failed to read next instance from Avro File Stream.";
  protected static final String AVRO_STREAM_UNSUPPORTED_METHOD = "This method is not supported for AvroFileStream yet.";

}
