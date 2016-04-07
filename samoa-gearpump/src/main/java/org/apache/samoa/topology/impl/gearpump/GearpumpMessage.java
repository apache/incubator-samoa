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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.utils.PartitioningScheme;

import java.io.Serializable;

public class GearpumpMessage implements Serializable {
  private ContentEvent event;
  private PartitioningScheme scheme;
  private String targetId;

  public GearpumpMessage() {
    this(null, null, null);
  }

  public GearpumpMessage(ContentEvent event, String targetId, PartitioningScheme scheme) {
    this.event = event;
    this.targetId = targetId;
    this.scheme = scheme;
  }

  public String getTargetId() {
    return targetId;
  }

  public void setTargetId(String targetId) {
    this.targetId = targetId;
  }

  public ContentEvent getEvent() {
    return event;
  }

  public void setEvent(ContentEvent event) {
    this.event = event;
  }

  public PartitioningScheme getScheme() {
    return scheme;
  }

  public void setScheme(PartitioningScheme scheme) {
    this.scheme = scheme;
  }
}
