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
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;

/**
 *
 * @author pwawrzyniak
 */
public class KafkaEntranceProcessor implements EntranceProcessor {

    transient private KafkaUtils kafkaUtils;
    List<byte[]> buffer;
    private final KafkaDeserializer deserializer;

    public KafkaEntranceProcessor(Properties props, String topic, int timeout, KafkaDeserializer deserializer) {
        this.kafkaUtils = new KafkaUtils(props, null, timeout);
        this.deserializer = deserializer;
    }

    private KafkaEntranceProcessor(KafkaUtils kafkaUtils, KafkaDeserializer deserializer) {
        this.kafkaUtils = kafkaUtils;
        this.deserializer = deserializer;
    }

    @Override
    public void onCreate(int id) {
        this.buffer = new ArrayList<>(100);
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
        return this.deserializer.deserialize(buffer.remove(buffer.size() - 1));

    }

    @Override
    public boolean process(ContentEvent event) {
        return false;
    }

    @Override
    public Processor newProcessor(Processor processor) {
        KafkaEntranceProcessor kep = (KafkaEntranceProcessor) processor;
        return new KafkaEntranceProcessor(new KafkaUtils(kep.kafkaUtils), deserializer);
    }

}
