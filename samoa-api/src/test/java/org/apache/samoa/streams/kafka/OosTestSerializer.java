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


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.samoa.instances.instances.Instance;
import org.apache.samoa.instances.kafka.KafkaDeserializer;
import org.apache.samoa.instances.kafka.KafkaSerializer;
import org.apache.samoa.learners.InstanceContentEvent;

/**
 *
 * @author Piotr Wawrzyniak
 */
public class OosTestSerializer implements KafkaDeserializer<Instance>, KafkaSerializer<Instance> {

    @Override
    public Instance deserialize(byte[] message) {
        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(message));
            Instance ice = (Instance)ois.readObject();
            return ice;
        } catch (IOException | ClassNotFoundException ex) {
            Logger.getLogger(OosTestSerializer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    public byte[] serialize(Instance message) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(message);
            oos.flush();
            return baos.toByteArray();            
        } catch (IOException ex) {
            Logger.getLogger(OosTestSerializer.class.getName()).log(Level.SEVERE, null, ex);
        }        
        return null;
    }
    
    
}
