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

package org.apache.samoa.topology.impl;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.topology.AbstractProcessingItem;
import org.apache.samoa.topology.ProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.impl.SamzaStream.SamzaSystemStream;
import org.apache.samoa.utils.PartitioningScheme;
import org.apache.samoa.utils.SamzaConfigFactory;
import org.apache.samoa.utils.StreamDestination;
import org.apache.samoa.utils.SystemsUtils;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

/**
 * ProcessingItem for Samza which is also a Samza task (StreamTask and InitableTask)
 * 
 * @author Anh Thu Vu
 */
public class SamzaProcessingItem extends AbstractProcessingItem
    implements SamzaProcessingNode, Serializable, StreamTask, InitableTask {

  /**
	 * 
	 */
  private static final long serialVersionUID = 1L;

  private Set<SamzaSystemStream> inputStreams; // input streams: system.stream
  private List<SamzaStream> outputStreams;

  /*
   * Constructors
   */
  // Need this so Samza can initialize a StreamTask
  public SamzaProcessingItem() {
  }

  /*
   * Implement org.apache.samoa.topology.ProcessingItem
   */
  public SamzaProcessingItem(Processor processor, int parallelismHint) {
    super(processor, parallelismHint);
    this.inputStreams = new HashSet<SamzaSystemStream>();
    this.outputStreams = new LinkedList<SamzaStream>();
  }

  /*
   * Simple setters, getters
   */
  public Set<SamzaSystemStream> getInputStreams() {
    return this.inputStreams;
  }

  /*
   * Extends AbstractProcessingItem
   */
  @Override
  protected ProcessingItem addInputStream(Stream inputStream, PartitioningScheme scheme) {
    SamzaSystemStream stream = ((SamzaStream) inputStream).addDestination(new StreamDestination(this, this
        .getParallelism(), scheme));
    this.inputStreams.add(stream);
    return this;
  }

  /*
   * Implement com.yahoo.samoa.topology.impl.SamzaProcessingNode
   */
  @Override
  public int addOutputStream(SamzaStream stream) {
    this.outputStreams.add(stream);
    return this.outputStreams.size();
  }

  public List<SamzaStream> getOutputStreams() {
    return this.outputStreams;
  }

  /*
   * Implement Samza task
   */
  @Override
  public void init(Config config, TaskContext context) throws Exception {
    String yarnConfHome = config.get(SamzaConfigFactory.YARN_CONF_HOME_KEY);
    if (yarnConfHome != null && yarnConfHome.length() > 0) // if the property is set, otherwise, assume we are running in local mode and ignore this                                                            // set , otherwise,
      SystemsUtils.setHadoopConfigHome(yarnConfHome);

    String filename = config.get(SamzaConfigFactory.FILE_KEY);
    String filesystem = config.get(SamzaConfigFactory.FILESYSTEM_KEY);
    this.setName(config.get(SamzaConfigFactory.JOB_NAME_KEY));
    SerializationProxy wrapper = (SerializationProxy) SystemsUtils.deserializeObjectFromFileAndKey(filesystem,
        filename, this.getName());
    this.setProcessor(wrapper.processor);
    this.outputStreams = wrapper.outputStreams;

    // Init Processor and Streams
    this.getProcessor().onCreate(0);
    for (SamzaStream stream : this.outputStreams) {
      stream.onCreate();
    }

  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
      throws Exception {
    for (SamzaStream stream : this.outputStreams) {
      stream.setCollector(collector);
    }
    this.getProcessor().process((ContentEvent) envelope.getMessage());
  }

  /*
   * SerializationProxy
   */
  private Object writeReplace() {
    return new SerializationProxy(this);
  }

  private static class SerializationProxy implements Serializable {
    /**
		 * 
		 */
    private static final long serialVersionUID = 1534643987559070336L;

    private Processor processor;
    private List<SamzaStream> outputStreams;

    public SerializationProxy(SamzaProcessingItem pi) {
      this.processor = pi.getProcessor();
      this.outputStreams = pi.getOutputStreams();
    }
  }

}
