package org.apache.samoa.apex.topology.impl;

import java.io.Serializable;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2016 Apache Software Foundation
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

import com.datatorrent.common.util.DefaultDelayOperator;
import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

@DefaultSerializer(JavaSerializer.class)
public class DelayOperatorSerializable<T> extends DefaultDelayOperator<T> implements Serializable {

  private static final long serialVersionUID = -2972537213450678368L;

  public final DefaultInputPortSerializable<T> input = new DefaultInputPortSerializable<T>() {

  private static final long serialVersionUID = 6830919916828325819L;

  @Override
  public void process(T tuple) {
      processTuple(tuple);
    }
  };
  
  public final DefaultOutputPortSerializable<T> output = new DefaultOutputPortSerializable<>();

  protected void processTuple(T tuple)
  {
    lastWindowTuples.add(tuple);
    output.emit(tuple);
  }

  @Override
  public void firstWindow()
  {
    for (T tuple : lastWindowTuples) {
      output.emit(tuple);
    }
  }

}
