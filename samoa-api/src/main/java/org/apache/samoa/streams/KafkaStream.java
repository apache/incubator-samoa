package org.apache.samoa.streams;

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

import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.samoa.instances.instances.Instance;
import org.apache.samoa.instances.instances.Instances;
import org.apache.samoa.instances.instances.InstancesHeader;
import org.apache.samoa.instances.kafka.KafkaAvroMapper;
import org.apache.samoa.instances.kafka.KafkaConsumerThread;
import org.apache.samoa.instances.kafka.KafkaDeserializer;
import org.apache.samoa.instances.loaders.AvroLoader;
import org.apache.samoa.instances.loaders.KafkaLoader;
import org.apache.samoa.instances.loaders.LoaderFactory;
import org.apache.samoa.instances.loaders.LoaderType;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.moa.core.Example;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.kafka.KafkaEntranceProcessor;
import org.apache.samoa.instances.kafka.KafkaJsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;

public class KafkaStream extends AbstractOptionHandler implements InstanceStream {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStream.class);
    private static final char TOPIC_SEPARATOR = '-';

    public StringOption hostOption = new StringOption("host", 'h', "Kafka host address", "127.0.0.1");
    public StringOption portOption = new StringOption("port", 'p', "Kafka port address", "9092");
    public StringOption topicOption = new StringOption("topic", 't', "Kafka topic name", "samoa_ff");
    public IntOption timeoutOption = new IntOption("timeout", 'e', "Kafka timeout", 1000, 0, Integer.MAX_VALUE);

    /**
     *
     */
    private static final long serialVersionUID = -4387950661589472853L;

    protected Instances instances;
    protected InstanceExample lastInstanceRead;

    protected boolean hitEndOfStream;

    private KafkaLoader kafkaLoader;

    @Override
    public InstancesHeader getHeader() {
        return new InstancesHeader(this.instances);
    }

    @Override
    public long estimatedRemainingInstances() {
        return -1;
    }

    @Override
    public boolean hasMoreInstances() {

        return true;
    }

    protected boolean readNextInstance() {
        logger.info("Reading next instance");

        this.instances = getDataset();
        if (this.instances != null) {
            this.lastInstanceRead = new InstanceExample(this.instances.instance(0));
            this.instances.delete(); // keep instances clean
            logger.info("Reading next instance successful");
            return true;
        }
        logger.info("Reading next instance unsuccessful");
        return false;

    }

    private KafkaDeserializer<Instance> getDeserializer(String value) {
        try {
            String topicExtension = value.substring(value.lastIndexOf(TOPIC_SEPARATOR) + 1);
            if(topicExtension.isEmpty())
                throw new InvalidTopicException("Invalid topic provided. Topic should contain an extension indicating data format, like topic-json or topic-avro.");
            switch (topicExtension){
                case "json":
                    return new KafkaJsonMapper(Charset.defaultCharset());
                case "avro":
                    return new KafkaAvroMapper();
                default:
                    throw new InvalidTopicException("Unsupported data serialization provided");
            }
        } catch(IndexOutOfBoundsException e){
            logger.error("Error parsing topic",e);
            throw new InvalidTopicException("Invalid topic provided. Topic should contain an extension indicating data format, like topic-json or topic-avro.");
        }

    }

    @Override
    public Example<Instance> nextInstance() {
        if (this.lastInstanceRead == null) {
            this.readNextInstance();
        }
        Example<Instance> ret = this.lastInstanceRead;
        this.lastInstanceRead = null;
        return ret;
    }

    @Override
    public boolean isRestartable() {
        return true;
    }

    @Override
    public void restart() {
        // TODO Auto-generated method stub
    }

    @Override
    public void getDescription(StringBuilder sb, int indent) {
        // TODO Auto-generated method stub
    }

    @Override
    protected void prepareForUseImpl(TaskMonitor monitor, ObjectRepository repository) {
        logger.info("Initializing KafkaLoader with host: " + this.hostOption.getValue() + ", port: " + this.portOption.getValue() + ", topic: " + this.topicOption.getValue());
        LoaderFactory loaderFactory = new LoaderFactory();
        // Create loader
        kafkaLoader = (KafkaLoader) loaderFactory.createLoader(LoaderType.KAFKA_LOADER, -1);

        // Create and set deserializer
        KafkaDeserializer<Instance> kafkaDeserializer = getDeserializer(topicOption.getValue());
        kafkaLoader.setDeserializer(kafkaDeserializer);

        // Create, configure and start data fetching thread
        kafkaLoader.setKafkaConsumerThread(new KafkaConsumerThread(getConsumerProperties(hostOption.getValue(), portOption.getValue()), Arrays.asList(topicOption.getValue()), timeoutOption.getValue()));
        kafkaLoader.runKafkaConsumerThread();
    }

    protected Properties getConsumerProperties(String BROKERHOST, String BROKERPORT) {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.setProperty("group.id", "test");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        return consumerProps;
    }

    private Instances getDataset() {
        Instance instance = kafkaLoader.readInstance();
        if (instance != null) {
            Instances ic = instance.dataset();
            ic.add(instance);
            return ic;
        } else {
            logger.info("hasNext returned false!");
            return null;
        }

    }

}
