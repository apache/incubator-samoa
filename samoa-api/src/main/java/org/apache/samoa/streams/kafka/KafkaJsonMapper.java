/*
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

import com.google.gson.*;
import org.apache.samoa.instances.instances.DenseInstanceData;
import org.apache.samoa.instances.instances.InstanceData;
import org.apache.samoa.learners.InstanceContentEvent;

import java.lang.reflect.Type;
import java.nio.charset.Charset;

//import org.apache.samoa.instances.SingleClassInstanceData;

/**
 * Sample class for serializing and deserializing {@link InstanceContentEvent}
 * from/to JSON format
 *
 * @author pwawrzyniak
 * @version 0.5.0-incubating-SNAPSHOT
 * @since 0.5.0-incubating
 */
public class KafkaJsonMapper implements KafkaDeserializer<InstanceContentEvent>, KafkaSerializer<InstanceContentEvent> {

    private final transient Gson gson;
    private final Charset charset;

    /**
     * Class constructor
     *
     * @param charset Charset to be used for bytes parsing
     */
    public KafkaJsonMapper(Charset charset) {
        this.gson = new GsonBuilder().registerTypeAdapter(InstanceData.class, new InstanceDataCustomDeserializer())
                .create();
        this.charset = charset;
    }

    @Override
    public InstanceContentEvent deserialize(byte[] message) {
        return gson.fromJson(new String(message, this.charset), InstanceContentEvent.class);
    }

    @Override
    public byte[] serialize(InstanceContentEvent message) {
        return gson.toJson(message).getBytes(this.charset);
    }

    //Unused
    public class InstanceDataCreator implements InstanceCreator<InstanceData> {
        @Override
        public InstanceData createInstance(Type type) {
            return new DenseInstanceData();
        }
    }

    public class InstanceDataCustomDeserializer implements JsonDeserializer<InstanceData> {

        @Override
        public InstanceData deserialize(JsonElement je, Type type, JsonDeserializationContext jdc) throws JsonParseException {
            double[] attributeValues = null;
            double classValues = 0.0d;
            JsonObject obj = (JsonObject) je;
            try {
                attributeValues = jdc.deserialize(obj.get("attributeValues"), double[].class);
            } catch (Exception e) {

            }
            try {
                classValues = jdc.deserialize(obj.get("classValue"), double.class);
            } catch (Exception e) {

            }
            if (attributeValues != null) {
                DenseInstanceData did = new DenseInstanceData(attributeValues);
                return did;
            } else {
                DenseInstanceData slid = new DenseInstanceData();
                slid.setValue(0, classValues);
                return slid;
            }
        }

    }

}
