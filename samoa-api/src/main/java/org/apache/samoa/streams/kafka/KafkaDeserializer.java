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


import org.apache.samoa.core.ContentEvent;

/**
 *
 * @author pwawrzyniak
 * @param <T> the class that would be deserialized
 */
public interface KafkaDeserializer<T extends ContentEvent> {
    
    // TODO: Consider key-value schema?
    /**
     * Method that provides deserialization algorithm
     * @param message Message as received from Apache Kafka
     * @return Deserialized form of message, to be passed to topology
     */
    T deserialize(byte[] message);
}
