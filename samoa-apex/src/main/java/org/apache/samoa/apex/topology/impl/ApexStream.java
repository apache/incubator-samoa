package org.apache.samoa.apex.topology.impl;

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

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import java.io.Serializable;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.topology.AbstractStream;

@DefaultSerializer(JavaSerializer.class)
public class ApexStream extends AbstractStream implements Serializable {

  private static final long serialVersionUID = -5712513402991550847L;

  private String streamId = "";
  public DefaultInputPortSerializable<ContentEvent> inputPort;
  public DefaultOutputPortSerializable<ContentEvent> outputPort;

  public ApexStream(String id) {
    streamId = id;
  }

  @Override
  public void put(ContentEvent contentEvent) {
    outputPort.emit(contentEvent);
  }

  @Override
  public String getStreamId() {
    return streamId;
  }

  @Override
  public void setBatchSize(int batchsize) {
  }
}
