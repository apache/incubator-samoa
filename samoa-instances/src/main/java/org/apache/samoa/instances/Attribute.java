/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.samoa.instances;

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

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author abifet
 */
public class Attribute implements Serializable {

  public static final String ARFF_ATTRIBUTE = "@attribute";
  public static final String ARFF_ATTRIBUTE_NUMERIC = "numeric";
  public static final String ARFF_ATTRIBUTE_NOMINAL = "nominal";
  public static final String ARFF_ATTRIBUTE_DATE = "date";

  /**
   *
   */
  protected boolean isNominal;
  /**
   *
   */
  protected boolean isNumeric;
  /**
   *
   */
  protected boolean isDate;
  /**
   *
   */
  protected String name;
  /**
   *
   */
  protected List<String> attributeValues;

  /**
   *
   * @return
   */
  public List<String> getAttributeValues() {
    return attributeValues;
  }

  /**
   *
   */
  protected int index;

  /**
   *
   * @param string
   */
  public Attribute(String string) {
    this.name = string;
    this.isNumeric = true;
  }

  /**
   *
   * @param attributeName
   * @param attributeValues
   */
  public Attribute(String attributeName, List<String> attributeValues) {
    this.name = attributeName;
    this.attributeValues = attributeValues;
    this.isNominal = true;
  }

  /**
   *
   */
  public Attribute() {
    this("");
  }

  /**
   *
   * @return
   */
  public boolean isNominal() {
    return this.isNominal;
  }

  /**
   *
   * @return
   */
  public String name() {
    return this.name;
  }

  /**
   *
   * @param value
   * @return
   */
  public String value(int value) {
    return attributeValues.get(value);
  }

  /**
   *
   * @return
   */
  public boolean isNumeric() {
    return isNumeric;
  }

  /**
   *
   * @return
   */
  public int numValues() {
    if (isNumeric()) {
      return 0;
    } else {
      return attributeValues.size();
    }
  }

  /**
   *
   * @return
   */
  public int index() { // RuleClassifier
    return this.index;
  }

  String formatDate(double value) {
    SimpleDateFormat sdf = new SimpleDateFormat();
    return sdf.format(new Date((long) value));
  }

  boolean isDate() {
    return isDate;
  }

  private Map<String, Integer> valuesStringAttribute;

  /**
   *
   * @param value
   * @return
   */
  public final int indexOfValue(String value) {

    if (isNominal() == false) {
      return -1;
    }
    if (this.valuesStringAttribute == null) {
      this.valuesStringAttribute = new HashMap<String, Integer>();
      int count = 0;
      for (String stringValue : attributeValues) {
        this.valuesStringAttribute.put(stringValue, count);
        count++;
      }
    }
    Integer val = (Integer) this.valuesStringAttribute.get(value);
    if (val == null) {
      return -1;
    } else {
      return val.intValue();
    }
  }

  @Override
  public String toString() {
    StringBuffer text = new StringBuffer();

    text.append(ARFF_ATTRIBUTE).append(" ").append(Utils.quote(this.name)).append(" ");

    if (isNominal) {
      text.append('{');
      Enumeration enu =  enumerateValues();
      while (enu.hasMoreElements()) {
        text.append(Utils.quote((String) enu.nextElement()));
        if (enu.hasMoreElements())
          text.append(',');
      }
      text.append('}');
    } else if (isNumeric) {
      text.append(ARFF_ATTRIBUTE_NUMERIC);
    } else if (isDate) {
      text.append(ARFF_ATTRIBUTE_DATE);
    }

    return text.toString();
  }

  /**
   * Returns an enumeration of all the attribute's values if the
   * attribute is nominal, null otherwise.
   *
   * @return enumeration of all the attribute's values
   */
  public final /*@ pure @*/ Enumeration enumerateValues() {

    if (this.isNominal()) {
      return Collections.enumeration(this.attributeValues);
    }
    return null;
  }
}
