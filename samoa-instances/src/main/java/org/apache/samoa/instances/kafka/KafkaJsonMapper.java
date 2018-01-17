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
package org.apache.samoa.instances.kafka;

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
import org.apache.samoa.instances.instances.DenseInstance;
import org.apache.samoa.instances.instances.DenseInstanceData;
import org.apache.samoa.instances.instances.Instance;
import org.apache.samoa.instances.instances.InstanceData;

import java.lang.reflect.Type;
import java.nio.charset.Charset;

/**
 * Sample class for serializing and deserializing {@link Instance}
 * from/to JSON format
 *
 * @author pwawrzyniak
 * @version 0.5.0-incubating-SNAPSHOT
 * @since 0.5.0-incubating
 */
public class KafkaJsonMapper implements KafkaDeserializer<Instance>, KafkaSerializer<Instance> {

    private final transient Gson gson;
    private final Charset charset;

    /**
     * Class constructor
     *
     * @param charset Charset to be used for bytes parsing
     */
    public KafkaJsonMapper(Charset charset) {
        this.gson = new GsonBuilder().registerTypeAdapter(InstanceData.class, new InstanceDataCustomDeserializer()).registerTypeAdapter(Instance.class,new InstanceDataCreator())
                .create();
        this.charset = charset;
    }

    @Override
    public Instance deserialize(byte[] message) {
        return gson.fromJson(new String(message, this.charset), Instance.class);
    }

    @Override
    public byte[] serialize(Instance message) {
        return gson.toJson(message).getBytes(this.charset);
    }



    //Unused
    public class InstanceDataCreator implements InstanceCreator<Instance> {
        @Override
        public Instance createInstance(Type type) {
            return new DenseInstance(0);
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
