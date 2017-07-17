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


import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.logging.Logger;

import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.streams.kafka.KafkaAvroMapper;
import org.junit.Test;

public class AvroSerializerDeserializerTest {

	private Logger logger = Logger.getLogger(AvroSerializerDeserializerTest.class.getName());
	public AvroSerializerDeserializerTest() {}
	
	@Test
	public void testAvroSerialize() {
		Random r = new Random();
        InstancesHeader header = TestUtilsForKafka.generateHeader(10);
        InstanceContentEvent eventToSerialize = TestUtilsForKafka.getData(r, 10, header);
		byte[] data = KafkaAvroMapper.avroSerialize(InstanceContentEvent.class, eventToSerialize);
		
		InstanceContentEvent eventDeserialized = KafkaAvroMapper.avroDeserialize(data, InstanceContentEvent.class);
		
		assertTrue("Serialized and deserialized event", isEqual(eventToSerialize, eventDeserialized));
		
	}
	
	public boolean isEqual(InstanceContentEvent a, InstanceContentEvent b) {
		if(a.getClassId() != b.getClassId()) {
			logger.info("a.getClassId() != b.getClassId(): " + (a.getClassId() != b.getClassId()));
			return false;
		}
		if(a.isLastEvent() != b.isLastEvent()) {
			logger.info("a.isLastEvent() != b.isLastEvent(): " + (a.isLastEvent() != b.isLastEvent()));
			return false;
		}
		if(a.isTesting() != b.isTesting()) {
			logger.info("a.isTesting() != b.isTesting(): " + (a.isTesting() != b.isTesting()));
			return false;
		}
		if(a.isTraining() != b.isTraining()) {
			logger.info("a.isTraining() != b.isTraining(): " + (a.isTraining() != b.isTraining()));
			return false;
		}
		if(a.getClassifierIndex() != b.getClassifierIndex()) {
			logger.info("a.getClassifierIndex() != b.getClassifierIndex(): " + (a.getClassifierIndex() != b.getClassifierIndex()));
			return false;
		}
		if(a.getEvaluationIndex() != b.getEvaluationIndex()) {
			logger.info("a.getEvaluationIndex() != b.getEvaluationIndex(): " + (a.getEvaluationIndex() != b.getEvaluationIndex()));
			return false;
		}
		if(a.getInstanceIndex() != b.getInstanceIndex()) {
			logger.info("a.getInstanceIndex() != b.getInstanceIndex(): " + (a.getInstanceIndex() != b.getInstanceIndex()));
			return false;
		}
		if(!a.getInstance().toString().equals(b.getInstance().toString())) {
			logger.info("a.getInstance().toString()!= b.getInstance().toString(): " + (a.getInstance().toString()!= b.getInstance().toString()));
			logger.info("a.toString(): " + a.getInstance().toString());
			logger.info("b.toString(): " + b.getInstance().toString());
			return false;
		}
		
		return true;
	}

}
