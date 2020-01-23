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

package org.apache.samoa.examples;

import java.util.Random;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;

/**
 * Example {@link EntranceProcessor} that generates a stream of random integers.
 */
public class HelloWorldSourceProcessor implements EntranceProcessor {

  private static final long serialVersionUID = 6212296305865604747L;
  private Random rnd;
  private final long maxInst;
  private long count;

  public HelloWorldSourceProcessor(long maxInst) {
    this.maxInst = maxInst;
  }

  @Override
  public boolean process(ContentEvent event) {
    // do nothing, API will be refined further
    return false;
  }

  @Override
  public void onCreate(int id) {
    rnd = new Random(id);
  }

  @Override
  public Processor newProcessor(Processor p) {
    HelloWorldSourceProcessor hwsp = (HelloWorldSourceProcessor) p;
    return new HelloWorldSourceProcessor(hwsp.maxInst);
  }

  @Override
  public boolean isFinished() {
    return count >= maxInst;
  }

  @Override
  public boolean hasNext() {
    return count < maxInst;
  }

  @Override
  public ContentEvent nextEvent() {
    count++;
    return new HelloWorldContentEvent(rnd.nextInt(), false);
  }
}
