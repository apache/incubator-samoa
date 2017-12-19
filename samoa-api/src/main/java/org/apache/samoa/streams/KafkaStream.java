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
import org.apache.samoa.instances.instances.Instance;
import org.apache.samoa.instances.instances.Instances;
import org.apache.samoa.instances.instances.InstancesHeader;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.moa.core.Example;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.kafka.KafkaEntranceProcessor;
import org.apache.samoa.streams.kafka.KafkaJsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Properties;

public class KafkaStream extends AbstractOptionHandler implements InstanceStream {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStream.class);

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

    private KafkaEntranceProcessor kep;

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
        logger.info("Initializing KEP with host: " + this.hostOption.getValue() + ", port: " + this.portOption.getValue() + ", topic: " + this.topicOption.getValue());
        kep = new KafkaEntranceProcessor(getConsumerProperties(this.hostOption.getValue(), this.portOption.getValue()), this.topicOption.getValue(), this.timeoutOption.getValue(), new KafkaJsonMapper(Charset.forName("UTF-8")));
        kep.onCreate(0);
        logger.info(kep != null ? "KEP is not null" : "KEP is null");
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
        if (kep.hasNext()) {
            InstanceContentEvent ice = (InstanceContentEvent) kep.nextEvent();
            Instances ic = ice.getInstance().dataset();
            ic.add(ice.getInstance());
            return ic;
        } else {
            logger.info("hasNext returned false!");
            return null;
        }

    }

}
