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

import org.apache.gearpump.Message;
import org.apache.gearpump.streaming.task.TaskContext;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.topology.AbstractStream;
import org.apache.samoa.topology.IProcessingItem;
import org.apache.samoa.utils.PartitioningScheme;

import java.io.Serializable;

public class GearpumpStream extends AbstractStream implements Serializable {

  private TaskContext taskContext;
  private String targetId;
  private IProcessingItem targetPi;
  private PartitioningScheme scheme;

  public GearpumpStream(IProcessingItem sourcePi) {
    super(sourcePi);
  }

  public void setTaskContext(TaskContext taskContext) {
    this.taskContext = taskContext;
  }

  public String getTargetId() {
    return targetId;
  }

  public void setTargetId(String targetId) {
    this.targetId = targetId;
  }

  public IProcessingItem getTargetPi() {
    return targetPi;
  }

  public void setTargetPi(IProcessingItem targetPi) {
    this.targetPi = targetPi;
  }

  public PartitioningScheme getScheme() {
    return scheme;
  }

  public void setScheme(PartitioningScheme scheme) {
    this.scheme = scheme;
  }

  @Override
  public void put(ContentEvent event) {
    GearpumpMessage message = new GearpumpMessage(event, targetId, scheme);
    taskContext.output(new Message(message, System.currentTimeMillis()));
  }

}
