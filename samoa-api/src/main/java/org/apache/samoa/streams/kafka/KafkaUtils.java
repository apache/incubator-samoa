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

import java.util.Collection;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

/**
 * Internal class responsible for Kafka Stream handling
 *
 * @author pwawrzyniak
 */
class KafkaUtils {

    // Consumer class for internal use to retrieve messages from Kafka
    private KafkaConsumer<String, byte[]> consumer;

    private KafkaProducer<String, byte[]> producer;

    // Properties of the consumer, as defined in Kafka documentation
    private Properties consumerProperties;
    private Properties producerProperties;

    // Batch size for Kafka Consumer    
    private int consumerTimeout;

    public KafkaUtils(Properties consumerProperties, Properties producerProperties, int consumerTimeout) {
        this.consumerProperties = consumerProperties;
        this.producerProperties = producerProperties;
        this.consumerTimeout = consumerTimeout;
    }

    public void initializeConsumer(Collection<String> topics) {
        // lazy initialization
        if (consumer == null) {
            consumer = new KafkaConsumer<String, byte[]>(consumerProperties);
        }
        consumer.subscribe(topics);
    }

    public ConsumerRecords<String, byte[]> getMessages() throws Exception {

        if (consumer != null) {
            if (!consumer.subscription().isEmpty()) {
                return consumer.poll(consumerTimeout);
            } else {
                // TODO: do it more elegant way
                throw new Exception("Consumer subscribed to no topics!");
            }
        } else {
            // TODO: do more elegant way
            throw new Exception("Consumer not initialised");
        }
    }
}
