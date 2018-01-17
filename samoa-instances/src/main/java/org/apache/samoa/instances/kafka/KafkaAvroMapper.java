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
package org.apache.samoa.instances.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.samoa.instances.instances.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Sample class for serializing and deserializing {@link Instance}
 * from/to Avro format
 *
 * @author Jakub Jankowski
 * @version 0.5.0-incubating-SNAPSHOT
 * @since 0.5.0-incubating
 */
public class KafkaAvroMapper implements KafkaDeserializer<Instance>, KafkaSerializer<Instance> {

    private static Logger logger = LoggerFactory.getLogger(KafkaAvroMapper.class);

    @Override
    public byte[] serialize(Instance message) {
        return avroSerialize(Instance.class, message);
    }

    @Override
    public Instance deserialize(byte[] message) {
        return avroDeserialize(message, Instance.class);
    }


    /**
     * Avro serialization based on specified schema
     * @param cls
     * @param v
     * @return
     */
    public static <V> byte[] avroSerialize(final Class<V> cls, final V v) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            Schema schema = new Schema.Parser().parse(KafkaAvroMapper.class.getResourceAsStream("/kafka.avsc"));
            DatumWriter<V> writer;

            if (v instanceof SpecificRecord) {
                writer = new SpecificDatumWriter<>(schema);
            } else {
                writer = new ReflectDatumWriter<>(schema);
            }

            BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
            writer.write(v, binEncoder);
            binEncoder.flush();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return bout.toByteArray();

    }

    /**
     * Avro deserialization based on specified schema
     * @param avroBytes
     * @param clazz
     * @return
     */
    public static <V> V avroDeserialize(byte[] avroBytes, Class<V> clazz) {
        V ret = null;
        try {
            Schema schema = new Schema.Parser().parse(KafkaAvroMapper.class.getResourceAsStream("/kafka.avsc"));
            ByteArrayInputStream in = new ByteArrayInputStream(avroBytes);
            DatumReader<V> reader = new SamoaDatumReader<>(schema);

            Decoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);

            ret = reader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    /**
     * Avro serialization using reflection
     * @param cls
     * @param v
     * @return
     */
    public static <V> byte[] toBytesGeneric(final Class<V> cls, final V v) {
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final Schema schema = ReflectData.AllowNull.get().getSchema(cls);
        final DatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
        final BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
        try {
            writer.write(v, binEncoder);
            binEncoder.flush();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return bout.toByteArray();
    }

}