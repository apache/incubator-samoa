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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Internal class responsible for Kafka Stream handling (both consume and produce)
 *
 * @author pwawrzyniak
 * @version 0.5.0-incubating-SNAPSHOT
 * @since 0.5.0-incubating
 */
class KafkaUtils {

    // Consumer class for internal use to retrieve messages from Kafka
    private KafkaConsumer<String, byte[]> consumer;

    private KafkaProducer<String, byte[]> producer;

    // Properties of the consumer, as defined in Kafka documentation
    private final Properties consumerProperties;
    private final Properties producerProperties;

    // Timeout for Kafka Consumer    
    private int consumerTimeout;

    /**
     * Class constructor
     * @param consumerProperties Properties of consumer
     * @param producerProperties Properties of producer
     * @param consumerTimeout Timeout for consumer poll requests
     */
    public KafkaUtils(Properties consumerProperties, Properties producerProperties, int consumerTimeout) {
        this.consumerProperties = consumerProperties;
        this.producerProperties = producerProperties;
        this.consumerTimeout = consumerTimeout;
    }

    KafkaUtils(KafkaUtils kafkaUtils) {
        this.consumerProperties = kafkaUtils.consumerProperties;
        this.producerProperties = kafkaUtils.producerProperties;
        this.consumerTimeout = kafkaUtils.consumerTimeout;
    }

    /**
     * Method used to initialize Kafka Consumer, i.e. instantiate it and subscribe to configured topic
     * @param topics List of Kafka topics that consumer should subscribe to
     */
    public void initializeConsumer(Collection<String> topics) {
        // lazy instantiation
        if (consumer == null) {
            consumer = new KafkaConsumer<>(consumerProperties);
        }
        consumer.subscribe(topics);
    }

    public void initializeProducer(){
        // lazy instantiation
        if(producer==null){
            producer = new KafkaProducer<>(producerProperties);
        }        
    }
    
    /**
     * Method for reading new messages from Kafka topics
     * @return Collection of read messages
     * @throws Exception Exception is thrown when consumer was not initialized or is not subscribed to any topic.
     */
    public List<byte[]> getKafkaMessages() throws Exception {

        if (consumer != null) {
            if (!consumer.subscription().isEmpty()) {
                return getMessagesBytes(consumer.poll(consumerTimeout));
            } else {
                // TODO: do it more elegant way
                throw new Exception("Consumer subscribed to no topics!");
            }
        } else {
            // TODO: do more elegant way
            throw new Exception("Consumer not initialised");
        }
    }

    private List<byte[]> getMessagesBytes(ConsumerRecords<String, byte[]> poll) {
        Iterator<ConsumerRecord<String, byte[]>> iterator = poll.iterator();
        List<byte[]> ret = new ArrayList<>();
        while(iterator.hasNext()){
            ret.add(iterator.next().value());
        }
        return ret;
    }
    
    public void sendKafkaMessage(String topic, byte[] message){
        if(producer!=null){
            producer.send(new ProducerRecord<String, byte[]>(topic, message));
            producer.flush();
        }
    }
}
