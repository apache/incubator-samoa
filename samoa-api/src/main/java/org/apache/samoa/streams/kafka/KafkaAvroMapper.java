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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.samoa.learners.InstanceContentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Sample class for serializing and deserializing {@link InstanceContentEvent}
 * from/to Avro format
 * 
 * @author Jakub Jankowski
 * @version 0.5.0-incubating-SNAPSHOT
 * @since 0.5.0-incubating
 */
public class KafkaAvroMapper implements KafkaDeserializer<InstanceContentEvent>, KafkaSerializer<InstanceContentEvent> {

	private static Logger logger = LoggerFactory.getLogger(KafkaAvroMapper.class);

	@Override
	public byte[] serialize(InstanceContentEvent message) {
		return toBytesGeneric(InstanceContentEvent.class, message);
	}

	@Override
	public InstanceContentEvent deserialize(byte[] message) {
		return avroDeserialize(message, InstanceContentEvent.class, null);
	}

	public static <T> byte[] avroSerialize(Class<T> clazz, Object object) {
		byte[] ret = null;
		try {
			if (object == null || !(object instanceof SpecificRecord)) {
				return null;
			}

			T record = (T) object;
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			Encoder e = EncoderFactory.get().directBinaryEncoder(out, null);
			SpecificDatumWriter<T> w = new SpecificDatumWriter<T>(clazz);
			w.write(record, e);
			e.flush();
			ret = out.toByteArray();
		} catch (IOException e) {

		}

		return ret;
	}

	public static <T> T avroDeserialize(byte[] avroBytes, Class<T> clazz, Schema schema) {
		T ret = null;
		try {
			ByteArrayInputStream in = new ByteArrayInputStream(avroBytes);
			Decoder d = DecoderFactory.get().directBinaryDecoder(in, null);
			SpecificDatumReader<T> reader = new SpecificDatumReader<T>(clazz);
			ret = reader.read(null, d);
		} catch (IOException e) {

		}

		return ret;
	}

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

	public static <V> byte[] avroBurrSerialize(final Class<V> cls, final V v) {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		try {
			Schema schema = new Schema.Parser().parse(new File("C:/java/avro/kafka.avsc"));
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

}
