package org.apache.samoa.instances.loaders;

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



import org.apache.samoa.instances.kafka.KafkaConsumerThread;
import org.apache.samoa.instances.instances.Instance;
import org.apache.samoa.instances.instances.InstanceInformation;
import org.apache.samoa.instances.kafka.KafkaDeserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaLoader implements Loader {

    private KafkaConsumerThread kafkaConsumerThread;
    private List<byte[]> buffer = new ArrayList<>(100);
    private KafkaDeserializer deserializer;

    public void setDeserializer(KafkaDeserializer deserializer) {
        this.deserializer = deserializer;
    }

    public void setKafkaConsumerThread(KafkaConsumerThread kafkaConsumerThread) {
        this.kafkaConsumerThread = kafkaConsumerThread;
    }

    public void runKafkaConsumerThread(){
        kafkaConsumerThread.start();
    }

    @Override
    public InstanceInformation getStructure() {
        return null;
    }

    @Override
    public Instance readInstance() {
        if(hasNext())
            return deserializer.deserialize(buffer.remove(0));
        return null;
    }


    public void close(){
        kafkaConsumerThread.close();
    }

    public boolean hasNext() {
        if (buffer.isEmpty()) {
            try {
                buffer.addAll(kafkaConsumerThread.getKafkaMessages());
            } catch (Exception ex) {
                Logger.getLogger(KafkaLoader.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return buffer.size() > 0;
    }

}
