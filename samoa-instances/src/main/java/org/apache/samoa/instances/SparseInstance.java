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

import java.text.SimpleDateFormat;

/**
 * 
 * @author abifet
 */
public class SparseInstance extends SingleLabelInstance {

  public SparseInstance(double d, double[] res) {
    super(d, res);
  }

  public SparseInstance(SingleLabelInstance inst) {
    super(inst);
  }

  public SparseInstance(double numberAttributes) {
    // super(1, new double[(int) numberAttributes-1]);
    super(1, null, null, (int) numberAttributes);
  }

  public SparseInstance(double weight, double[] attributeValues, int[] indexValues, int numberAttributes) {
    super(weight, attributeValues, indexValues, numberAttributes);
  }

  @Override
  public String toString() {
    StringBuffer str = new StringBuffer();

    str.append("{");

    for (int i=0; i<this.numAttributes()-1;i++){
      if (!this.isMissing(i)) {

        //if the attribute is Nominal we print the string value of the attribute.
        if (this.attribute(i).isNominal()) {
          int valueIndex = (int) this.value(i);
          String stringValue = this.attribute(i).value(valueIndex);
          str.append(i).append(" ").append(stringValue).append(",");
        } else if (this.attribute(i).isNumeric()) {
          //if the attribute is numeric we print the value of the attribute only if it is not equal 0
          if (this.value(i) != 0) {
            str.append(i).append(" ").append(this.value(i)).append(",");
          }
        } else if (this.attribute(i).isDate()) {
          SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
          str.append(i).append(" ").append(dateFormatter.format(this.value(i))).append(",");
        }
      } else { //represent missing values
        str.append(i).append(" ").append("?,");
      }
    }
    //append the class value at the end of the instance.
    str.append(classIndex()).append(" ").append(this.classAttribute().value((int)classValue()));

    str.append("}");

    return str.toString();
  }
}
