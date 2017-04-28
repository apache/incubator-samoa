/*
 * Copyright 2017 The Apache Software Foundation.
 *
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
 */
package org.apache.samoa.streams.kafka;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2017 Apache Software Foundation
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


import java.util.Properties;
import java.util.Random;
import org.apache.samoa.instances.Attribute;
import org.apache.samoa.instances.DenseInstance;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.moa.core.FastVector;

/**
 *
 * @author pwawrzyniak <your.name at your.org>
 */
public class TestUtilsForKafka {

    private static final String ZKHOST = "10.255.251.202"; 		//10.255.251.202
    private static final String BROKERHOST = "10.255.251.214";	//10.255.251.214
    private static final String BROKERPORT = "6667";		//6667, local: 9092
    private static final String TOPIC = "samoa_test";				//samoa_test, local: test

    protected static InstanceContentEvent getData(Random instanceRandom, int numAtts, InstancesHeader header) {
        double[] attVals = new double[numAtts + 1];
        double sum = 0.0;
        double sumWeights = 0.0;
        for (int i = 0; i < numAtts; i++) {
            attVals[i] = instanceRandom.nextDouble();
//            sum += this.weights[i] * attVals[i];
//            sumWeights += this.weights[i];
        }
        int classLabel;
        if (sum >= sumWeights * 0.5) {
            classLabel = 1;
        } else {
            classLabel = 0;
        }

        Instance inst = new DenseInstance(1.0, attVals);
        inst.setDataset(header);
        inst.setClassValue(classLabel);

        return new InstanceContentEvent(0, inst, true, false);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected static InstancesHeader generateHeader(int numAttributes) {
        FastVector attributes = new FastVector();
        for (int i = 0; i < numAttributes; i++) {
            attributes.addElement(new Attribute("att" + (i + 1)));
        }

        FastVector classLabels = new FastVector();
        for (int i = 0; i < numAttributes; i++) {
            classLabels.addElement("class" + (i + 1));
        }
        attributes.addElement(new Attribute("class", classLabels));
        InstancesHeader streamHeader = new InstancesHeader(new Instances("test-kafka", attributes, 0));
        streamHeader.setClassIndex(streamHeader.numAttributes() - 1);
        return streamHeader;
    }

    
        protected static Properties getProducerProperties() {
        return getProducerProperties("test");
    }
    
    /**
     *
     * @param clientId
     * @return
     */
    protected static Properties getProducerProperties(String clientId) {
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.setProperty("group.id", "test");
        producerProps.setProperty("client.id", clientId);
        return producerProps;
    }

    protected static Properties getConsumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.setProperty("group.id", "test");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        //consumerProps.setProperty("client.id", "consumer0");
        return consumerProps;
    }
    
    protected static Properties getConsumerProducerProperties() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.setProperty("group.id", "burrito");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("client.id", "burrito");
        return props;
    }
}
