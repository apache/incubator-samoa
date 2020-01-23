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

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.samoa.topology.impl;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.topology.AbstractProcessingItem;
import org.apache.samoa.topology.IProcessingItem;
import org.apache.samoa.topology.ProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.utils.PartitioningScheme;
import org.apache.samoa.utils.StreamDestination;

/**
 * 
 * @author abifet
 */
class SimpleProcessingItem extends AbstractProcessingItem {
  private IProcessingItem[] arrayProcessingItem;

  SimpleProcessingItem(Processor processor) {
    super(processor);
  }

  SimpleProcessingItem(Processor processor, int parallelism) {
    super(processor);
    this.setParallelism(parallelism);
  }

  public IProcessingItem getProcessingItem(int i) {
    return arrayProcessingItem[i];
  }

  @Override
  protected ProcessingItem addInputStream(Stream inputStream, PartitioningScheme scheme) {
    StreamDestination destination = new StreamDestination(this, this.getParallelism(), scheme);
    ((SimpleStream) inputStream).addDestination(destination);
    return this;
  }

  public SimpleProcessingItem copy() {
    Processor processor = this.getProcessor();
    return new SimpleProcessingItem(processor.newProcessor(processor));
  }

  public void processEvent(ContentEvent event, int counter) {

    int parallelism = this.getParallelism();
    // System.out.println("Process event "+event+" (isLast="+event.isLastEvent()+") with counter="+counter+" while parallelism="+parallelism);
    if (this.arrayProcessingItem == null && parallelism > 0) {
      // Init processing elements, the first time they are needed
      this.arrayProcessingItem = new IProcessingItem[parallelism];
      for (int j = 0; j < parallelism; j++) {
        arrayProcessingItem[j] = this.copy();
        arrayProcessingItem[j].getProcessor().onCreate(j);
      }
    }
    if (this.arrayProcessingItem != null) {
      IProcessingItem pi = this.getProcessingItem(counter);
      Processor p = pi.getProcessor();
      // System.out.println("PI="+pi+", p="+p);
      this.getProcessingItem(counter).getProcessor().process(event);
    }
  }
}
