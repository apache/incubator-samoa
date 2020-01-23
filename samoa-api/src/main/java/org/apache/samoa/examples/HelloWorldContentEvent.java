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

import org.apache.samoa.core.ContentEvent;

/**
 * Example {@link ContentEvent} that contains a single integer.
 */
public class HelloWorldContentEvent implements ContentEvent {

  private static final long serialVersionUID = -2406968925730298156L;
  private final boolean isLastEvent;
  private final int helloWorldData;

  public HelloWorldContentEvent(int helloWorldData, boolean isLastEvent) {
    this.isLastEvent = isLastEvent;
    this.helloWorldData = helloWorldData;
  }

  /*
   * No-argument constructor for Kryo
   */
  public HelloWorldContentEvent() {
    this(0, false);
  }

  @Override
  public String getKey() {
    return null;
  }

  @Override
  public void setKey(String str) {
    // do nothing, it's key-less content event
  }

  @Override
  public boolean isLastEvent() {
    return isLastEvent;
  }

  public int getHelloWorldData() {
    return helloWorldData;
  }

  @Override
  public String toString() {
    return "HelloWorldContentEvent [helloWorldData=" + helloWorldData + "]";
  }
}
