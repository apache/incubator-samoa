package org.apache.samoa.streams.kafka;

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

import org.apache.samoa.instances.DenseInstance;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.instances.SparseInstance;

public class KafkaToInstanceMapper {

    public Instance getInstance(String message, String keyValueSeparator, String valuesSeparator, int instanceType, int numAttr, InstancesHeader header) {
        Instance inst;
        if (!message.isEmpty()) {
            String[] KeyValueString = message.split(keyValueSeparator);
            String[] attributes = KeyValueString[1].split(valuesSeparator);
            inst = createInstance(header, instanceType);
            for (int i = 0; i < attributes.length - 1; i++) {
                if (i < numAttr) {
                    inst.setValue(i, Double.parseDouble(attributes[i]));
                }
            }
            inst.setDataset(header);
            inst.setClassValue(Double
                    .parseDouble(attributes[attributes.length - 1]));
            return inst;
        }
        throw new IllegalArgumentException("Empty string value from Kafka");
    }

    public Instance createInstance(InstancesHeader header, int instanceType) {
        Instance inst;
        if (instanceType == 0)
            inst = new DenseInstance(header.numAttributes());
        else
            inst = new SparseInstance(header.numAttributes());
        return inst;
    }
}
