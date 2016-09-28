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

import org.apache.samoa.core.EntranceProcessor;

public class EntranceProcessingItemTask extends Task {
  EntranceProcessor entranceProcessor;
  private TaskContext taskContext;
  private UserConfig userConfig;
  private GearpumpStream outputStream;

  public EntranceProcessingItemTask(TaskContext taskContext, UserConfig userConf) {
    super(taskContext, userConf);
    this.taskContext = taskContext;
    this.userConfig = userConf;
    byte[] bytes = userConf.getBytes(Utils.entrancePiConf).get();
    GearpumpEntranceProcessingItem entranceProcessingItem =
        ((GearpumpEntranceProcessingItem) Utils.bytesToObject(bytes));
    this.entranceProcessor = entranceProcessingItem.getProcessor();
    this.outputStream = entranceProcessingItem.getStream();
  }

  @Override
  public void onStart(StartTime startTime) {
    outputStream.setTaskContext(this.taskContext);

    entranceProcessor.onCreate(taskContext.taskId().index());
    self().tell(new Message("start", System.currentTimeMillis()), self());
  }

  @Override
  public void onNext(Message msg) {
    if (entranceProcessor.hasNext()) {
      GearpumpMessage message =
          new GearpumpMessage(entranceProcessor.nextEvent(), outputStream.getTargetId(),
              outputStream.getScheme());
      taskContext.output(new Message(message, System.currentTimeMillis()));
    }
    self().tell(new Message("continue", System.currentTimeMillis()), self());
  }

  @Override
  public void onStop() {
    super.onStop();
  }

}

