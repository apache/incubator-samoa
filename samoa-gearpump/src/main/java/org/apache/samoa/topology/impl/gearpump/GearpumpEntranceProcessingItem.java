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

import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.streaming.Processor;

import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.topology.AbstractEntranceProcessingItem;
import org.apache.samoa.topology.Stream;

import java.io.IOException;
import java.io.Serializable;

public class GearpumpEntranceProcessingItem extends AbstractEntranceProcessingItem
    implements TopologyNode, Serializable {

  private GearpumpStream stream;

  public GearpumpEntranceProcessingItem(EntranceProcessor entranceProcessor) {
    super(entranceProcessor);
    this.setName(entranceProcessor.getClass().getName());
  }

  @Override
  public GearpumpStream createStream() {
    GearpumpStream stream = new GearpumpStream(this);
    this.stream = stream;
    return stream;
  }

  @Override
  public Processor createGearpumpProcessor() {
    byte[] bytes = Utils.objectToBytes(this);
    UserConfig userConfig = UserConfig.empty().withBytes(Utils.entrancePiConf, bytes);
    return new org.apache.gearpump.streaming.Processor.DefaultProcessor<>(
        1, this.getName(), userConfig, EntranceProcessingItemTask.class);
  }

  public GearpumpStream getStream() {
    return stream;
  }

  private void writeObject(java.io.ObjectOutputStream stream)
      throws IOException {
    stream.writeObject(getProcessor());
    stream.writeObject(getName());
    stream.writeObject(getOutputStream());
    stream.writeObject(this.stream);
  }

  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    setProcessor((org.apache.samoa.core.EntranceProcessor) stream.readObject());
    setName((String) stream.readObject());
    setOutputStream((Stream) stream.readObject());
    this.stream = (GearpumpStream) stream.readObject();
  }

}
