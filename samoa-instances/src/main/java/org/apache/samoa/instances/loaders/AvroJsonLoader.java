package org.apache.samoa.instances.loaders;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.samoa.instances.instances.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroJsonLoader extends AvroLoader {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LoggerFactory.getLogger(AvroJsonLoader.class);

  /** The Character reader for JSON read */
  protected Reader reader = null;

  public AvroJsonLoader(InputStream inputStream, int classAttribute) {
    super(classAttribute);
    initializeSchema(inputStream);
  }

  public AvroJsonLoader( int classAttribute) {
    super(classAttribute);
  }

  /* (non-Javadoc)
   * @see org.apache.samoa.instances.loaders.AvroLoader#initializeSchema(java.io.InputStream)
   */
  @Override
  public void initializeSchema(InputStream inputStream)
  {
    String schemaString = null;
    try {
      this.reader = new BufferedReader(new InputStreamReader(inputStream));
      schemaString = ((BufferedReader) this.reader).readLine();
      this.schema = new Schema.Parser().parse(schemaString);
      this.datumReader = new GenericDatumReader<GenericRecord>(schema);
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
   * @see org.apache.samoa.instances.loaders.AvroLoader#readInstance()
   */
  @Override
  public Instance readInstance() {

    String line = null;
    Decoder decoder = null;
    GenericRecord record = null;

    try {
      while ((line = ((BufferedReader) reader).readLine()) != null) {
        if (line.trim().length() <= 0)
          continue;

        decoder = DecoderFactory.get().jsonDecoder(schema, line);
        record = datumReader.read(null, decoder);
        break;
      }
    } catch (IOException ioException) {
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
    if (reader != null)
      try {
        reader.close();
      } catch (IOException ioException) {
        logger.error(AVRO_LOADER_INSTANCE_READ_ERROR + " : {}", ioException);
        throw new RuntimeException(AVRO_LOADER_INSTANCE_READ_ERROR + " : " + ioException);
      }
  }
}
