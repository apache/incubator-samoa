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

import java.util.List;
import java.util.concurrent.ExecutorService;

import mockit.Expectations;
import mockit.Mocked;
import mockit.NonStrictExpectations;
import mockit.Tested;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.topology.impl.SimpleProcessingItem;
import org.apache.samoa.topology.impl.SimpleStream;
import org.apache.samoa.utils.PartitioningScheme;
import org.apache.samoa.utils.StreamDestination;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Anh Thu Vu
 * 
 */
public class SimpleProcessingItemTest {

  @Tested
  private SimpleProcessingItem pi;

  @Mocked
  private Processor processor;
  @Mocked
  private SimpleStream stream;
  @Mocked
  private StreamDestination destination;
  @Mocked
  private ContentEvent event;

  private final int parallelism = 4;
  private final int counter = 2;

  @Before
  public void setUp() throws Exception {
    pi = new SimpleProcessingItem(processor, parallelism);
  }

  @Test
  public void testConstructor() {
    assertSame("Processor was not set correctly.", processor, pi.getProcessor());
    assertEquals("Parallelism was not set correctly.", parallelism, pi.getParallelism(), 0);
  }

  @Test
  public void testConnectInputShuffleStream() {
    new Expectations() {
      {
        destination = new StreamDestination(pi, parallelism, PartitioningScheme.SHUFFLE);
        stream.addDestination(destination);
      }
    };
    pi.connectInputShuffleStream(stream);
  }

  @Test
  public void testConnectInputKeyStream() {
    new Expectations() {
      {
        destination = new StreamDestination(pi, parallelism, PartitioningScheme.GROUP_BY_KEY);
        stream.addDestination(destination);
      }
    };
    pi.connectInputKeyStream(stream);
  }

  @Test
  public void testConnectInputAllStream() {
    new Expectations() {
      {
        destination = new StreamDestination(pi, parallelism, PartitioningScheme.BROADCAST);
        stream.addDestination(destination);
      }
    };
    pi.connectInputAllStream(stream);
  }

  @Test
  public void testProcessEvent() {
    new Expectations() {
      {
        for (int i = 0; i < parallelism; i++) {
          processor.newProcessor(processor);
          result = processor;

          processor.onCreate(anyInt);
        }

        processor.process(event);
      }
    };
    pi.processEvent(event, counter);

  }

}
