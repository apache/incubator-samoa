package org.apache.samoa.instances.instances;

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

import org.apache.samoa.instances.Attribute;

import java.io.Serializable;

public interface Instance extends Serializable {

  /**
   * Gets the weight of the instance.
   *
   * @return the weight
   */
  double weight();

  /**
   * Sets the weight.
   *
   * @param weight the new weight
   */
  void setWeight(double weight);

  /**
   * Attribute.
   *
   * @param instAttIndex the inst att index
   * @return the attribute
   */
  Attribute attribute(int instAttIndex);

  /**
   * Delete attribute at.
   *
   * @param i the index
   */
  void deleteAttributeAt(int i);

  /**
   * Insert attribute at.
   *
   * @param i the index
   */
  void insertAttributeAt(int i);

  /**
   * Gets the number of attributes.
   *
   * @return the number of attributes
   */
  int numAttributes();

  /**
   * Adds the sparse values.
   *
   * @param indexValues the index values
   * @param attributeValues the attribute values
   * @param numberAttributes the number attributes
   */
  void addSparseValues(int[] indexValues, double[] attributeValues, int numberAttributes);

  /**
   * Gets the number of values, mainly for sparse instances.
   *
   * @return the number of values
   */
  int numValues();

  /**
   * Gets the value of a discrete attribute as a string.
   *
   * @param i the i
   * @return the string
   */
  String stringValue(int i);

  /**
   * Gets the value of an attribute.
   *
   * @param instAttIndex the inst att index
   * @return the double
   */
  double value(int instAttIndex);

  /**
   * Sets an attribute as missing
   *
   * @param instAttIndex, the attribute's index     
   */
  void setMissing(int instAttIndex);

  /**
   * Sets the value of an attribute.
   *
   * @param instAttIndex the index
   * @param value the value
   */
  void setValue(int instAttIndex, double value);

  /**
   * Checks if an attribute is missing.
   *
   * @param instAttIndex the inst att index
   * @return true, if is missing
   */
  boolean isMissing(int instAttIndex);

  /**
   * Gets the index of the attribute given the index of the array in a sparse
   * representation.
   *
   * @param arrayIndex the index of the array
   * @return the index
   */
  int index(int arrayIndex);

  /**
   * Gets the value of an attribute in a sparse representation of the
   * instance.
   *
   * @param i the i
   * @return the value
   */
  double valueSparse(int i);

  /**
   * Checks if the attribute is missing sparse.
   *
   * @param p1 the p1
   * @return true, if is missing sparse
   */
  boolean isMissingSparse(int p1);

  /**
   * To double array.
   *
   * @return the double[]
   */
  double[] toDoubleArray();

  /**
   * Class attribute.
   *
   * @return the attribute
   */
  Attribute classAttribute();

  /**
   * Class index.
   *
   * @return the int
   */
  int classIndex();

  /**
   * Class is missing.
   *
   * @return true, if successful
   */
  boolean classIsMissing();

  /**
   * Class value.
   *
   * @return the double
   */
  double classValue();

  /**
   * Num classes.
   *
   * @return the int
   */
  int numClasses();

  /**
   * Sets the class value.
   *
   * @param d the new class value
   */
  void setClassValue(double d);

  /**
   * Copy.
   *
   * @return the instance
   */
  Instance copy();

  /**
   * Sets the dataset.
   *
   * @param dataset the new dataset
   */
  void setDataset(Instances dataset);

  /**
   * Dataset.
   *
   * @return the instances
   */
  Instances dataset();

  /**
   * Gets the number of input attributes.
   *
   * @return the number of input attributes
   */
  int numInputAttributes();

  /**
   * Gets the number of output attributes.
   *
   * @return the number of output attributes
   */
  int numOutputAttributes();

  /**
   * Gets the number of output attributes.
   *
   * @return the number of output attributes
   */
  int numberOutputTargets();

  /**
   * Gets the value of an output attribute.
   *
   * @param attributeIndex the index
   * @return the value
   */
  double classValue(int attributeIndex);

  /**
   * Sets the value of an output attribute.
   *
   * @param indexClass the output attribute index
   * @param valueAttribute the value of the attribute
   */
  void setClassValue(int indexClass, double valueAttribute);

  /**
   * Gets an output attribute given its index.
   *
   * @param attributeIndex the index
   * @return the attribute
   */
  Attribute outputAttribute(int attributeIndex);

  /**
   * Gets an input attribute given its index.
   *
   * @param attributeIndex the index
   * @return the attribute
   */
  Attribute inputAttribute(int attributeIndex);

  /**
   * Gets the value of an input attribute.
   *
   * @param attributeIndex the index
   * @return the value
   */
  double valueInputAttribute(int attributeIndex);

  /**
   * Gets the value of an output attribute.
   *
   * @param attributeIndex the index
   * @return the value
   */
  double valueOutputAttribute(int attributeIndex);

  /**
   * Index of an Attribute.
   *
   * @param attribute, the attribute to be found.
   * @return the index of an attribute
   */
  int indexOfAttribute(Attribute attribute);

  /**
   * Gets the value of an attribute, given the attribute.
   *
   * @param attribute the attribute
   * @return the double
   */
  double value(Attribute attribute);

  /**
   * Sets an attribute as missing
   *
   * @param attribute, the Attribute
   */
  void setMissing(Attribute attribute);

  /**
   * Sets the value of an attribute.
   *
   * @param attribute, the Attribute
   * @param value the value
   */
  void setValue(Attribute attribute, double value);

  /**
   * Checks if an attribute is missing.
   *
   * @param attribute, the Attribute
   * @return true, if is missing
   */
  boolean isMissing(Attribute attribute);



}
