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

package org.apache.samoa.instances;

import java.io.Serializable;
import java.util.List;

/**
 * The Class InstanceInformation.
 */
public class InstanceInformation implements Serializable {

  /**
   * The dataset's name.
   */
  protected String relationName;

  protected AttributesInformation attributesInformation;

  /**
   * The class index.
   */
  protected int classIndex = Integer.MAX_VALUE; //By default is multilabel

  /**
   * Range for multi-label instances.
   */
  protected Range range;

  /**
   * Instantiates a new instance information.
   *
   * @param chunk
   *          the chunk
   */
  public InstanceInformation(InstanceInformation chunk) {
    this.relationName = chunk.relationName;
    this.attributesInformation = chunk.attributesInformation;
    this.classIndex = chunk.classIndex;
    this.range = chunk.range;
  }

  /**
   * Instantiates a new instance information.
   *
   * @param st    the st
   * @param input the input
   */
  public InstanceInformation(String st, Attribute[] input) {
    this.relationName = st;
    this.attributesInformation = new AttributesInformation(input, input.length);
  }

  /**
   * Instantiates a new instance information.
   *
   * @param st
   *          the st
   * @param input
   *          the input
   */
  public InstanceInformation(String st, List<Attribute> input) {
    this.relationName = st;
    this.attributesInformation = new AttributesInformation(input, input.size());
  }

  /**
   * Instantiates a new instance information.
   */
  public InstanceInformation() {
    this.relationName = null;
    this.attributesInformation = null;
  }

  public Attribute inputAttribute(int w) {
    return this.attributesInformation.attribute(inputAttributeIndex(w));
  }

  public Attribute outputAttribute(int w) {
    return this.attributesInformation.attribute(outputAttributeIndex(w));
  }


  public String getRelationName() {
    return this.relationName;
  }

  public void setRelationName(String string) {
    this.relationName = string;
  }

  public int classIndex() {
    return this.classIndex;
  }

  public void setClassIndex(int classIndex) {
    this.classIndex = classIndex;
  }

  public Attribute classAttribute() {
    return this.attribute(this.classIndex());
  }

  public int numAttributes() {
    return this.attributesInformation.numberAttributes;
  }

  public Attribute attribute(int w) {
    return this.attributesInformation.attribute(w);
  }

  public int numClasses() {
    return this.attributesInformation.attribute(classIndex()).numValues();
  }

  public void deleteAttributeAt(int integer) {
    this.attributesInformation.deleteAttributeAt(integer);
    if (this.classIndex > integer) {
      this.classIndex--;
    }
  }

  public void insertAttributeAt(Attribute attribute, int i) {
    this.attributesInformation.insertAttributeAt(attribute, i);
    if (this.classIndex >= i) {
      this.classIndex++;
    }
  }

  public void setAttributes(Attribute[] v) {
    if (this.attributesInformation == null)
      this.attributesInformation = new AttributesInformation();
    this.attributesInformation.setAttributes(v);
  }

  public int inputAttributeIndex(int index) {
    int ret = 0;
    if (classIndex == Integer.MAX_VALUE) {//Multi Label
      if (index < range.getStart())//JD
        ret = index;
      else
        ret = index + range.getSelectionLength();

    } else { //Single Label
      ret = classIndex() > index ? index : index + 1;
    }
    return ret;
  }

  public int outputAttributeIndex(int attributeIndex) {
    int ret = 0;
    if (classIndex == Integer.MAX_VALUE) {//Multi Label
      ret = attributeIndex + range.getStart(); //JD - Range should be a "block"
    } else { //Single Label
      ret = classIndex;
    }
    return ret;
  }

  public int numInputAttributes() {
    int ret = 0;
    if (classIndex == Integer.MAX_VALUE) {//Multi Label
      ret = this.numAttributes() - range.getSelectionLength(); //JD
    } else { //Single Label
      ret = this.numAttributes() - 1;
    }
    return ret;
  }

  public int numOutputAttributes() {
    int ret = 0;
    if (classIndex == Integer.MAX_VALUE) {//Multi Label
      ret = range.getSelectionLength(); //JD
    } else { //Single Label
      ret = 1;
    }
    return ret;
  }

  public void setRangeOutputIndices(Range range) {
    this.setClassIndex(Integer.MAX_VALUE);
    this.range = range;
  }

  public void setAttributes(Attribute[] v, int[] indexValues) {
    if (this.attributesInformation == null)
      this.attributesInformation = new AttributesInformation();
    this.attributesInformation.setAttributes(v, indexValues);

  }

}
