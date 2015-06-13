package org.apache.samoa.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 Yahoo! Inc.
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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;

/**
 * Lightweight replicas of ThreadProcessingItem. ThreadsProcessingItem manages a list of these objects and assigns each
 * incoming message to be processed by one of them.
 * 
 * @author Anh Thu Vu
 * 
 */
public class ThreadsProcessingItemInstance {

  private Processor processor;
  private int threadIndex;

  public ThreadsProcessingItemInstance(Processor processor, int threadIndex) {
    this.processor = processor;
    this.threadIndex = threadIndex;
  }

  public int getThreadIndex() {
    return this.threadIndex;
  }

  public Processor getProcessor() {
    return this.processor;
  }

  public void processEvent(ContentEvent event) {
    this.processor.process(event);
  }
}
