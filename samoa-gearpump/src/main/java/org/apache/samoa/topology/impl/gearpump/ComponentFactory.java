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

import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;
import org.apache.samoa.topology.EntranceProcessingItem;
import org.apache.samoa.topology.IProcessingItem;
import org.apache.samoa.topology.ProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;

public class ComponentFactory implements org.apache.samoa.topology.ComponentFactory {
  @Override
  public ProcessingItem createPi(Processor processor) {
    return createPi(processor, 1);
  }

  @Override
  public ProcessingItem createPi(Processor processor, int parallelism) {
    return new GearpumpProcessingItem(processor, parallelism);
  }

  @Override
  public EntranceProcessingItem createEntrancePi(EntranceProcessor
      entranceProcessor) {
    return new GearpumpEntranceProcessingItem(entranceProcessor);
  }

  @Override
  public Stream createStream(IProcessingItem sourcePi) {
    TopologyNode topologyNode = (TopologyNode) sourcePi;
    return topologyNode.createStream();
  }

  @Override
  public Topology createTopology(String topoName) {
    return new GearpumpTopology(topoName);
  }
}
