package org.apache.samoa.streams;

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

/**
 * License
 */

import org.apache.samoa.instances.Instance;
import org.apache.samoa.moa.core.Example;
import org.apache.samoa.moa.streams.InstanceStream;

/**
 * The Class StreamSource.
 */
public class StreamSource implements java.io.Serializable {

  /**
	 * 
	 */
  private static final long serialVersionUID = 3974668694861231236L;

  /**
   * Instantiates a new stream source.
   * 
   * @param stream
   *          the stream
   */
  public StreamSource(InstanceStream stream) {
    super();
    this.stream = stream;
  }

  /** The stream. */
  protected InstanceStream stream;

  /**
   * Gets the stream.
   * 
   * @return the stream
   */
  public InstanceStream getStream() {
    return stream;
  }

  /**
   * Next instance.
   * 
   * @return the instance
   */
  public Example<Instance> nextInstance() {
    return stream.nextInstance();
  }

  /**
   * Sets the stream.
   * 
   * @param stream
   *          the new stream
   */
  public void setStream(InstanceStream stream) {
    this.stream = stream;
  }

  /**
   * Checks for more instances.
   * 
   * @return true, if successful
   */
  public boolean hasMoreInstances() {
    return this.stream.hasMoreInstances();
  }

}
