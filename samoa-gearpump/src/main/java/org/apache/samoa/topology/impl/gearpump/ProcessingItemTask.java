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
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.streaming.task.StartTime;
import org.apache.gearpump.streaming.task.Task;
import org.apache.gearpump.streaming.task.TaskContext;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;

import java.util.Set;

public class ProcessingItemTask extends Task {

  private TaskContext taskContext;
  private UserConfig userConfig;
  private Processor processor;
  private Set<GearpumpStream> streams;

  public ProcessingItemTask(TaskContext taskContext, UserConfig userConf) {
    super(taskContext, userConf);
    this.taskContext = taskContext;
    this.userConfig = userConf;
    byte[] bytes = userConf.getBytes(Utils.piConf).get();
    GearpumpProcessingItem pi = (GearpumpProcessingItem) Utils.bytesToObject(bytes);
    this.processor = pi.getProcessor();
    this.streams = pi.getStreams();
  }

  public void setProcessor(Processor processor) {
    this.processor = processor;
  }

  @Override
  public void onStart(StartTime startTime) {
    for (GearpumpStream stream : streams) {
      stream.setTaskContext(this.taskContext);
    }

    processor.onCreate(taskContext.taskId().index());
  }

  @Override
  public void onNext(Message msg) {
    GearpumpMessage message = (GearpumpMessage) msg.msg();
    String targetId = message.getTargetId();
    if (targetId.equals(this.processor.getClass().getSimpleName())) {
      ContentEvent event = message.getEvent();
      processor.process(event);
    }
  }

  @Override
  public void onStop() {
  }
}
