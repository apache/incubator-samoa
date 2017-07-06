package org.apache.samoa.instances;

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

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroBinaryLoader extends AvroLoader {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory.getLogger(AvroBinaryLoader.class);

  /** Avro Binary reader for an input stream **/
  protected DataFileStream<GenericRecord> dataFileStream = null;

  public AvroBinaryLoader(InputStream inputStream, int classAttribute) {
    super(classAttribute);
    initializeSchema(inputStream);
  }

  /* (non-Javadoc)
   * @see org.apache.samoa.instances.AvroLoader#initializeSchema(java.io.InputStream)
   */
  @Override
  public void initializeSchema(InputStream inputStream)
  {
    try {
      this.datumReader = new GenericDatumReader<GenericRecord>();
      this.dataFileStream = new DataFileStream<GenericRecord>(inputStream, datumReader);
      this.schema = dataFileStream.getSchema();

      this.instanceInformation = getHeader();
      this.isSparseData = isSparseData();

      if (classAttribute < 0) {
        this.instanceInformation.setClassIndex(this.instanceInformation.numAttributes() - 1);
      } else if (classAttribute > 0) {
        this.instanceInformation.setClassIndex(classAttribute - 1);
      }

    } catch (IOException ioException) {
      logger.error(AVRO_LOADER_SCHEMA_READ_ERROR + " : {}", ioException);
      throw new RuntimeException(AVRO_LOADER_SCHEMA_READ_ERROR + " : " + ioException);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.samoa.instances.AvroLoader#readInstance()
   */
  @Override
  public Instance readInstance() {

    GenericRecord record = null;

    try {
      if (dataFileStream.hasNext()) {
        record = (GenericRecord) dataFileStream.next();
      }
    } catch (Exception ioException) {
      logger.error(AVRO_LOADER_INSTANCE_READ_ERROR + " : {}", ioException);
      throw new RuntimeException(AVRO_LOADER_INSTANCE_READ_ERROR + " : " + ioException);
    }

    if (record == null)
    {
      closeReader();
      return null;
    }

    if (isSparseData)
      return readInstanceSparse(record);

    return readInstanceDense(record);
  }

  /**
   * Close the Avro Data Stream
   */
  private void closeReader()
  {
    if (dataFileStream != null)
      try {
        dataFileStream.close();
      } catch (IOException ioException) {
        logger.error(AVRO_LOADER_INSTANCE_READ_ERROR + " : {}", ioException);
        throw new RuntimeException(AVRO_LOADER_INSTANCE_READ_ERROR + " : " + ioException);
      }
  }
}
