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

/*
 *    NumericAttributeBinaryRulePredicate.java
 *    Copyright (C) 2013 University of Porto, Portugal
 *    @author E. Almeida, A. Carvalho, J. Gama
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 *    
 */
package org.apache.samoa.moa.classifiers.rules.core.conditionaltests;

import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.moa.classifiers.core.conditionaltests.InstanceConditionalBinaryTest;
import org.apache.samoa.moa.classifiers.rules.core.Predicate;

/**
 * Numeric binary conditional test for instances to use to split nodes in AMRules.
 * 
 * @version $Revision: 1 $
 */
public class NumericAttributeBinaryRulePredicate extends InstanceConditionalBinaryTest implements Predicate {

  private static final long serialVersionUID = 1L;

  protected int attIndex;

  protected double attValue;

  protected int operator; // 0 =, 1<=, 2>

  public NumericAttributeBinaryRulePredicate() {
    this(0, 0, 0);
  }

  public NumericAttributeBinaryRulePredicate(int attIndex, double attValue,
      int operator) {
    this.attIndex = attIndex;
    this.attValue = attValue;
    this.operator = operator;
  }

  public NumericAttributeBinaryRulePredicate(NumericAttributeBinaryRulePredicate oldTest) {
    this(oldTest.attIndex, oldTest.attValue, oldTest.operator);
  }

  @Override
  public int branchForInstance(Instance inst) {
    int instAttIndex = this.attIndex < inst.classIndex() ? this.attIndex
        : this.attIndex + 1;
    if (inst.isMissing(instAttIndex)) {
      return -1;
    }
    double v = inst.value(instAttIndex);
    int ret = 0;
    switch (this.operator) {
    case 0:
      ret = (v == this.attValue) ? 0 : 1;
      break;
    case 1:
      ret = (v <= this.attValue) ? 0 : 1;
      break;
    case 2:
      ret = (v > this.attValue) ? 0 : 1;
    }
    return ret;
  }

  /**
     *
     */
  @Override
  public String describeConditionForBranch(int branch, InstancesHeader context) {
    if ((branch >= 0) && (branch <= 2)) {
      String compareChar = (branch == 0) ? "=" : (branch == 1) ? "<=" : ">";
      return InstancesHeader.getAttributeNameString(context,
          this.attIndex)
          + ' '
          + compareChar
          + InstancesHeader.getNumericValueString(context,
              this.attIndex, this.attValue);
    }
    throw new IndexOutOfBoundsException();
  }

  /**
     *
     */
  @Override
  public void getDescription(StringBuilder sb, int indent) {
    // TODO Auto-generated method stub
  }

  @Override
  public int[] getAttsTestDependsOn() {
    return new int[] { this.attIndex };
  }

  public double getSplitValue() {
    return this.attValue;
  }

  @Override
  public boolean evaluate(Instance inst) {
    return (branchForInstance(inst) == 0);
  }

  @Override
  public String toString() {
    if ((operator >= 0) && (operator <= 2)) {
      String compareChar = (operator == 0) ? "=" : (operator == 1) ? "<=" : ">";
      // int equalsBranch = this.equalsPassesTest ? 0 : 1;
      return "x" + this.attIndex
          + ' '
          + compareChar
          + ' '
          + this.attValue;
    }
    throw new IndexOutOfBoundsException();
  }

  public boolean isEqual(NumericAttributeBinaryRulePredicate predicate) {
    return (this.attIndex == predicate.attIndex
        && this.attValue == predicate.attValue
        && this.operator == predicate.operator);
  }

  public boolean isUsingSameAttribute(NumericAttributeBinaryRulePredicate predicate) {
    return (this.attIndex == predicate.attIndex
    && this.operator == predicate.operator);
  }

  public boolean isIncludedInRuleNode(
      NumericAttributeBinaryRulePredicate predicate) {
    boolean ret;
    if (this.operator == 1) { // <=
      ret = (predicate.attValue <= this.attValue);
    } else { // >
      ret = (predicate.attValue > this.attValue);
    }

    return ret;
  }

  public void setAttributeValue(
      NumericAttributeBinaryRulePredicate ruleSplitNodeTest) {
    this.attValue = ruleSplitNodeTest.attValue;

  }
}
