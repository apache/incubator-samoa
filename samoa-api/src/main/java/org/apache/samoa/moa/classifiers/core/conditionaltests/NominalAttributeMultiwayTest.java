package org.apache.samoa.moa.classifiers.core.conditionaltests;

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

import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.InstancesHeader;

/**
 * Nominal multi way conditional test for instances to use to split nodes in Hoeffding trees.
 * 
 * @author Richard Kirkby (rkirkby@cs.waikato.ac.nz)
 * @version $Revision: 7 $
 */
public class NominalAttributeMultiwayTest extends InstanceConditionalTest {

  private static final long serialVersionUID = 1L;

  protected int attIndex;

  public NominalAttributeMultiwayTest(int attIndex) {
    this.attIndex = attIndex;
  }

  @Override
  public int branchForInstance(Instance inst) {
    int instAttIndex = this.attIndex; // < inst.classIndex() ? this.attIndex
    // : this.attIndex + 1;
    return inst.isMissing(instAttIndex) ? -1 : (int) inst.value(instAttIndex);
  }

  @Override
  public String describeConditionForBranch(int branch, InstancesHeader context) {
    return InstancesHeader.getAttributeNameString(context, this.attIndex)
        + " = "
        + InstancesHeader.getNominalValueString(context, this.attIndex,
            branch);
  }

  @Override
  public int maxBranches() {
    return -1;
  }

  @Override
  public void getDescription(StringBuilder sb, int indent) {
    // TODO Auto-generated method stub
  }

  @Override
  public int[] getAttsTestDependsOn() {
    return new int[] { this.attIndex };
  }
}
