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

import static org.junit.Assert.*;
import mockit.Mocked;
import mockit.Tested;
import mockit.Verifications;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.topology.impl.ThreadsEventRunnable;
import org.apache.samoa.topology.impl.ThreadsProcessingItemInstance;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Anh Thu Vu
 * 
 */
public class ThreadsEventRunnableTest {

  @Tested
  private ThreadsEventRunnable task;

  @Mocked
  private ThreadsProcessingItemInstance piInstance;
  @Mocked
  private ContentEvent event;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    task = new ThreadsEventRunnable(piInstance, event);
  }

  @Test
  public void testConstructor() {
    assertSame("WorkerProcessingItem is not set correctly.", piInstance, task.getWorkerProcessingItem());
    assertSame("ContentEvent is not set correctly.", event, task.getContentEvent());
  }

  @Test
  public void testRun() {
    task.run();
    new Verifications() {
      {
        piInstance.processEvent(event);
        times = 1;
      }
    };
  }

}
