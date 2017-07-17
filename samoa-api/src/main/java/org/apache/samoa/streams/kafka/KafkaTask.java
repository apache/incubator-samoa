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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.ComponentFactory;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javacliparser.Configurable;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;

/**
 * Kafka task
 * 
 * @author Jakub Jankowski
 * @version 0.5.0-incubating-SNAPSHOT
 * @since 0.5.0-incubating
 *
 */

public class KafkaTask implements Task, Configurable {

	private static final long serialVersionUID = 3984474041982397855L;
	private static Logger logger = LoggerFactory.getLogger(KafkaTask.class);
	
	//czy identyczne dla enterance i destination?
	Properties producerProps;
	Properties consumerProps;
	int timeout;
	private final KafkaDeserializer deserializer;
	private final KafkaSerializer serializer;
	private final String topic;

	private TopologyBuilder builder;
	private Topology kafkaTopology;

	public IntOption kafkaParallelismOption = new IntOption("parallelismOption", 'p',
			"Number of destination Processors", 1, 1, Integer.MAX_VALUE);

	public StringOption evaluationNameOption = new StringOption("evaluationName", 'n', "Identifier of the evaluation",
			"KafkaTask" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

	/**
     * Class constructor
     * @param props Properties of Kafka Producer and Consumer
     * @see <a href="http://kafka.apache.org/documentation/#producerconfigs">Kafka Producer configuration</a>
     * @see <a href="http://kafka.apache.org/documentation/#consumerconfigs">Kafka Consumer configuration</a>
     * @param topic Topic to which destination processor will write into
     * @param timeout Timeout used when polling Kafka for new messages
     * @param serializer Implementation of KafkaSerializer that handles arriving data serialization
     * @param serializer Implementation of KafkaDeserializer that handles arriving data deserialization
     */
	public KafkaTask(Properties producerProps, Properties consumerProps, String topic, int timeout, KafkaSerializer serializer, KafkaDeserializer deserializer) {
		this.producerProps = producerProps;
		this.consumerProps = consumerProps;
		this.deserializer = deserializer;
		this.serializer = serializer;
		this.topic = topic;
		this.timeout = timeout;
	}

	@Override
	public void init() {
		logger.info("Invoking init");
		if (builder == null) {
			builder = new TopologyBuilder();
			logger.info("Successfully instantiating TopologyBuilder");

			builder.initTopology(evaluationNameOption.getValue());
			logger.info("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());
		}
		
		// create enterance processor
		KafkaEntranceProcessor sourceProcessor = new KafkaEntranceProcessor(consumerProps, topic, timeout, deserializer);
		builder.addEntranceProcessor(sourceProcessor);
		
		// create stream
		Stream stream = builder.createStream(sourceProcessor);
		
		// create destination processor
		KafkaDestinationProcessor destProcessor = new KafkaDestinationProcessor(producerProps, topic, serializer);
		builder.addProcessor(destProcessor, kafkaParallelismOption.getValue());
		builder.connectInputShuffleStream(stream, destProcessor);
		
		// build topology
		kafkaTopology = builder.build();
	    logger.info("Successfully built the topology");
	}

	@Override
	public Topology getTopology() {
		return kafkaTopology;
	}

	@Override
	public void setFactory(ComponentFactory factory) {
		logger.info("Invoking setFactory: "+factory.toString());
		builder = new TopologyBuilder(factory);
	    logger.info("Successfully instantiating TopologyBuilder");

	    builder.initTopology(evaluationNameOption.getValue());
	    logger.info("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());

	}

}
