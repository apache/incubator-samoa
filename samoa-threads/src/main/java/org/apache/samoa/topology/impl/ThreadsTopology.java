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

import org.apache.samoa.topology.AbstractTopology;
import org.apache.samoa.topology.IProcessingItem;

/**
 * Topology for multithreaded engine.
 * 
 * @author Anh Thu Vu
 * 
 */
public class ThreadsTopology extends AbstractTopology {
  ThreadsTopology(String name) {
    super(name);
  }

  public void run() {
    if (this.getEntranceProcessingItems() == null)
      throw new IllegalStateException("You need to set entrance PI before running the topology.");
    if (this.getEntranceProcessingItems().size() != 1)
      throw new IllegalStateException("ThreadsTopology supports 1 entrance PI only. Number of entrance PIs is "
          + this.getEntranceProcessingItems().size());

    this.setupProcessingItemInstances();
    ThreadsEntranceProcessingItem entrancePi = (ThreadsEntranceProcessingItem) this.getEntranceProcessingItems()
        .toArray()[0];
    if (entrancePi == null)
      throw new IllegalStateException("You need to set entrance PI before running the topology.");
    entrancePi.getProcessor().onCreate(0); // id=0 as it is not used in simple mode
    entrancePi.startSendingEvents();
  }

  /*
   * Tell all the ThreadsProcessingItems to create & init their replicas
   * (ThreadsProcessingItemInstance)
   */
  private void setupProcessingItemInstances() {
    for (IProcessingItem pi : this.getProcessingItems()) {
      if (pi instanceof ThreadsProcessingItem) {
        ThreadsProcessingItem tpi = (ThreadsProcessingItem) pi;
        tpi.setupInstances();
      }
    }
  }
}
