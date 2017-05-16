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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 *
 * @author pwawrzyniak
 */
class KafkaConsumerThread extends Thread {

    // Consumer class for internal use to retrieve messages from Kafka
    private transient KafkaConsumer<String, byte[]> consumer;

    private Logger log = Logger.getLogger(KafkaConsumerThread.class.getName());

    private final Properties consumerProperties;
    private final Collection<String> topics;
    private final long consumerTimeout;
    private final List<byte[]> buffer;
    // used to synchronize things
    private final Object lock;
    private boolean running;

    /**
     * Class constructor
     *
     * @param consumerProperties Properties of Consumer
     * @param topics Topics to fetch (subscribe)
     * @param consumerTimeout Timeout for data polling
     */
    KafkaConsumerThread(Properties consumerProperties, Collection<String> topics, long consumerTimeout) {
        this.running = false;
        this.consumerProperties = consumerProperties;
        this.topics = topics;
        this.consumerTimeout = consumerTimeout;
        this.buffer = new ArrayList<>();
        lock = new Object();
    }

    @Override
    public void run() {

        initializeConsumer();

        while (running) {
            fetchDataFromKafka();
        }

        cleanUp();
    }

    /**
     * Method for fetching data from Apache Kafka. It takes care of received
     * data
     */
    private void fetchDataFromKafka() {
        if (consumer != null) {
            if (!consumer.subscription().isEmpty()) {
                try {
                    List<byte[]> kafkaMsg = getMessagesBytes(consumer.poll(consumerTimeout));
                    fillBufferAndNotifyWaits(kafkaMsg);
                } catch (Throwable t) {
                    Logger.getLogger(KafkaConsumerThread.class.getName()).log(Level.SEVERE, null, t);
                }
            }
        }
    }

    /**
     * Copies received messages to class buffer and notifies Processor to grab
     * the data.
     *
     * @param kafkaMsg Messages received from Kafka
     */
    private void fillBufferAndNotifyWaits(List<byte[]> kafkaMsg) {
        synchronized (lock) {
            buffer.addAll(kafkaMsg);
            if (buffer.size() > 0) {
                lock.notifyAll();
            }
        }
    }

    private void cleanUp() {
        // clean resources
        if (consumer != null) {
            consumer.unsubscribe();
            consumer.close();
        }
    }

    private void initializeConsumer() {
        // lazy instantiation
        log.log(Level.INFO, "Instantiating Kafka consumer");
        if (consumer == null) {
            consumer = new KafkaConsumer<>(consumerProperties);
            running = true;
        }
        consumer.subscribe(topics);
    }

    private List<byte[]> getMessagesBytes(ConsumerRecords<String, byte[]> poll) {
        Iterator<ConsumerRecord<String, byte[]>> iterator = poll.iterator();
        List<byte[]> ret = new ArrayList<>();
        while (iterator.hasNext()) {
            ret.add(iterator.next().value());
        }
        return ret;
    }

    void close() {
        running = false;
    }

    List<byte[]> getKafkaMessages() {
        synchronized (lock) {
            if (buffer.isEmpty()) {
                try {
                    // block the call until new messages are received
                    lock.wait();
                } catch (InterruptedException ex) {
                    Logger.getLogger(KafkaConsumerThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            ArrayList<byte[]> ret = new ArrayList<>();
            // copy buffer to return list
            ret.addAll(buffer);
            // clear message buffer
            buffer.clear();
            return ret;
        }
    }
}
