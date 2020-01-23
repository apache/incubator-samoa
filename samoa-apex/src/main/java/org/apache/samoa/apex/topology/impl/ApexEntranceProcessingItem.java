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

package org.apache.samoa.apex.topology.impl;

import java.util.UUID;

import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.topology.AbstractEntranceProcessingItem;

/**
 * EntranceProcessingItem implementation for Apache Apex.
 */
public class ApexEntranceProcessingItem extends AbstractEntranceProcessingItem implements ApexTopologyNode {

  private final ApexInputOperator inputOperator;
  private int numStreams;

  public ApexEntranceProcessingItem() {
    inputOperator = null;
  }

  // Constructor
  public ApexEntranceProcessingItem(EntranceProcessor processor) {
    this(processor, UUID.randomUUID().toString());
  }

  // Constructor
  public ApexEntranceProcessingItem(EntranceProcessor processor, String friendlyId) {
    super(processor);
    this.setName(friendlyId);
    this.inputOperator = new ApexInputOperator(processor);
  }

  @Override
  public void addToTopology(ApexTopology topology, int parallelismHint) {
    topology.getDAG().addOperator(this.getName(), inputOperator);
    //add num partitions
  }

  @Override
  public ApexStream createStream() {
    return inputOperator.createStream("Stream_from_" + this.getName() + "_#" + numStreams++);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.insert(0, String.format("id: %s, ", this.getName()));
    return sb.toString();
  }
}
