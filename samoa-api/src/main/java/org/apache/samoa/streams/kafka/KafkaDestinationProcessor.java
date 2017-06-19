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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;

/**
 * Destination processor that writes data to Apache Kafka
 * @author pwawrzyniak
 * @version 0.5.0-incubating-SNAPSHOT
 * @since 0.5.0-incubating
 */
public class KafkaDestinationProcessor implements Processor {

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        kafkaUtils.closeProducer();
    }

    private final KafkaUtils kafkaUtils;
    private final String topic;
    private final KafkaSerializer serializer;

    /**
     * Class constructor
     * @param props Properties of Kafka Producer
     * @see <a href="http://kafka.apache.org/documentation/#producerconfigs">Kafka Producer configuration</a>
     * @param topic Topic this destination processor will write into
     * @param serializer Implementation of KafkaSerializer that handles arriving data serialization
     */
    public KafkaDestinationProcessor(Properties props, String topic, KafkaSerializer serializer) {
        this.kafkaUtils = new KafkaUtils(null, props, 0);
        this.topic = topic;
        this.serializer = serializer;
    }
    
    private KafkaDestinationProcessor(KafkaUtils kafkaUtils, String topic, KafkaSerializer serializer){
        this.kafkaUtils = kafkaUtils;
        this.topic = topic;
        this.serializer = serializer;
    }

    @Override
    public boolean process(ContentEvent event) {
        try {
            kafkaUtils.sendKafkaMessage(topic, serializer.serialize(event));
        } catch (Exception ex) {
            Logger.getLogger(KafkaEntranceProcessor.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    @Override
    public void onCreate(int id) {
        kafkaUtils.initializeProducer();
    }

    @Override
    public Processor newProcessor(Processor processor) {
        KafkaDestinationProcessor kdp = (KafkaDestinationProcessor)processor;
        return new KafkaDestinationProcessor(new KafkaUtils(kdp.kafkaUtils), kdp.topic, kdp.serializer);
    }

}
