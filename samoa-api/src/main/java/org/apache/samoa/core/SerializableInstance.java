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

package org.apache.samoa.core;

import org.apache.samoa.instances.DenseInstance;
import org.apache.samoa.instances.Instance;

/**
 * The Class SerializableInstance. This class is needed for serialization of kryo
 */
public class SerializableInstance extends DenseInstance {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = -3659459626274566468L;

  /**
   * Instantiates a new serializable instance.
   */
  public SerializableInstance() {
    super(0);
  }

  /**
   * Instantiates a new serializable instance.
   * 
   * @param arg0
   *          the arg0
   */
  public SerializableInstance(int arg0) {
    super(arg0);
  }

  /**
   * Instantiates a new serializable instance.
   * 
   * @param inst
   *          the inst
   */
  public SerializableInstance(Instance inst) {
    super(inst);
  }

}
