/*
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;

/**
 * Entrance processor that reads incoming messages from <a href="https://kafka.apache.org/">Apache Kafka</a>
 * @author pwawrzyniak
 * @version 0.5.0-incubating-SNAPSHOT
 * @since 0.5.0-incubating
 */
public class KafkaEntranceProcessor implements EntranceProcessor {

    transient private final KafkaUtils kafkaUtils;
    private List<byte[]> buffer;
    private final KafkaDeserializer deserializer;
    private final String topic;

    /**
     * Class constructor
     * @param props Properties of Kafka consumer
     * @see  <a href="https://kafka.apache.org/documentation/#newconsumerconfigs"> Apache Kafka consumer configuration</a>
     * @param topic Topic from which the messages should be read
     * @param timeout Timeout used when polling Kafka for new messages
     * @param deserializer Instance of the implementation of {@link KafkaDeserializer}
     */
    public KafkaEntranceProcessor(Properties props, String topic, int timeout, KafkaDeserializer deserializer) {
        this.kafkaUtils = new KafkaUtils(props, null, timeout);
        this.deserializer = deserializer;
        this.topic = topic;
    }

    private KafkaEntranceProcessor(KafkaUtils kafkaUtils, KafkaDeserializer deserializer, String topic) {
        this.kafkaUtils = kafkaUtils;
        this.deserializer = deserializer;
        this.topic = topic;
    }

    @Override
    public void onCreate(int id) {
        this.buffer = new ArrayList<>(100);
        this.kafkaUtils.initializeConsumer(Arrays.asList(this.topic));
    }

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public boolean hasNext() {
        if (buffer.isEmpty()) {
            try {
                buffer.addAll(kafkaUtils.getKafkaMessages());
            } catch (Exception ex) {
                Logger.getLogger(KafkaEntranceProcessor.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return buffer.size() > 0;
    }

    @Override
    public ContentEvent nextEvent() {
        // assume this will never be called when buffer is empty!        
        return this.deserializer.deserialize(buffer.remove(0));
    }

    @Override
    public boolean process(ContentEvent event) {
        return false;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        KafkaEntranceProcessor kep = (KafkaEntranceProcessor) processor;
        return new KafkaEntranceProcessor(new KafkaUtils(kep.kafkaUtils), kep.deserializer, kep.topic);
    }

    @Override
    protected void finalize() throws Throwable {
        kafkaUtils.closeConsumer();
        super.finalize();
    }
    
}
