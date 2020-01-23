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

import org.apache.samoa.core.ContentEvent;

/**
 * Runnable class where each object corresponds to a ContentEvent and an assigned PI. When a PI receives a ContentEvent,
 * it will create a ThreadsEventRunnable with the received ContentEvent and an assigned workerPI. This runnable is then
 * submitted to a thread queue waiting to be executed. The worker PI will process the received event when the runnable
 * object is executed/run.
 * 
 * @author Anh Thu Vu
 * 
 */
public class ThreadsEventRunnable implements Runnable {

  private ThreadsProcessingItemInstance workerPi;
  private ContentEvent event;

  public ThreadsEventRunnable(ThreadsProcessingItemInstance workerPi, ContentEvent event) {
    this.workerPi = workerPi;
    this.event = event;
  }

  public ThreadsProcessingItemInstance getWorkerProcessingItem() {
    return this.workerPi;
  }

  public ContentEvent getContentEvent() {
    return this.event;
  }

  @Override
  public void run() {
    try {
      workerPi.processEvent(event);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
