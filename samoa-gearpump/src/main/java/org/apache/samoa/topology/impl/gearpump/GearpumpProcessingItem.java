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

import org.apache.samoa.core.Processor;
import org.apache.samoa.topology.AbstractProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.utils.PartitioningScheme;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class GearpumpProcessingItem extends AbstractProcessingItem implements TopologyNode, Serializable {
  private static final long serialVersionUID = -9066409791668954099L;
  private Set<GearpumpStream> streams;

  public GearpumpProcessingItem(Processor processor, int parallelism) {
    super(processor, parallelism);
    this.setName(processor.getClass().getSimpleName());
    this.streams = new HashSet<>();
  }

  @Override
  protected org.apache.samoa.topology.ProcessingItem addInputStream(Stream inputStream,
      PartitioningScheme scheme) {
    ((GearpumpStream) inputStream).setTargetPi(this);
    ((GearpumpStream) inputStream).setScheme(scheme);
    ((GearpumpStream) inputStream).setTargetId(this.getName());

    return this;
  }

  @Override
  public GearpumpStream createStream() {
    GearpumpStream stream = new GearpumpStream(this);
    streams.add(stream);
    return stream;
  }

  @Override
  public org.apache.gearpump.streaming.Processor createGearpumpProcessor() {
    byte[] bytes = Utils.objectToBytes(this);
    UserConfig userConfig = UserConfig.empty().withBytes(Utils.piConf, bytes);
    return new org.apache.gearpump.streaming.Processor.DefaultProcessor<>(
        this.getParallelism(), this.getName(), userConfig, ProcessingItemTask.class);
  }

  public Set<GearpumpStream> getStreams() {
    return streams;
  }

  private void writeObject(java.io.ObjectOutputStream stream)
      throws IOException {
    stream.writeObject(getProcessor());
    stream.writeObject(getName());
    stream.writeInt(getParallelism());
    stream.writeObject(streams);
  }

  private void readObject(java.io.ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    setProcessor((Processor) stream.readObject());
    setName((String) stream.readObject());
    setParallelism(stream.readInt());
    streams = (Set<GearpumpStream>) stream.readObject();
  }

}
