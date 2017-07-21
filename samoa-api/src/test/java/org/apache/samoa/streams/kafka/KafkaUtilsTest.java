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


import com.google.gson.Gson;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.samoa.instances.InstancesHeader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author pwawrzyniak
 */
@Ignore
public class KafkaUtilsTest {

    private static final String ZKHOST = "10.255.251.202"; 		//10.255.251.202
    private static final String BROKERHOST = "10.255.251.214";	//10.255.251.214
    private static final String BROKERPORT = "6667";		//6667, local: 9092
    private static final String TOPIC_R = "test-r";
    private static final String TOPIC_S = "test-s";

    private static KafkaServer kafkaServer;
    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static String zkConnect;

    private Logger logger = Logger.getLogger(KafkaUtilsTest.class.getCanonicalName());
    private long CONSUMER_TIMEOUT = 1000;

    public KafkaUtilsTest() {
    }

    @BeforeClass
    public static void setUpClass() throws IOException {
        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        zkConnect = ZKHOST + ":" + "2181"; //+ zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        /*Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafkaUtils-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);*/

        // create topics
        //AdminUtils.createTopic(zkUtils, TOPIC_R, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        //AdminUtils.createTopic(zkUtils, TOPIC_S, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

    }

    @AfterClass
    public static void tearDownClass() {
        //kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of initializeConsumer method, of class KafkaUtils.
     */
    @Test
    public void testInitializeConsumer() throws Exception {
        logger.log(Level.INFO, "initializeConsumer");
        Collection<String> topics = Arrays.asList(TOPIC_R);
        KafkaUtils instance = new KafkaUtils(TestUtilsForKafka.getConsumerProperties(), TestUtilsForKafka.getProducerProperties(), CONSUMER_TIMEOUT);
        assertNotNull(instance);

        instance.initializeConsumer(topics);

        assertNotNull(instance.getKafkaMessages());
        instance.closeConsumer();
    }

    /**
     * Test of getKafkaMessages method, of class KafkaUtils.
     */
    @Test
    public void testGetKafkaMessages() throws Exception {
        logger.log(Level.INFO, "getKafkaMessages");
        Collection<String> topics = Arrays.asList(TOPIC_R);
        KafkaUtils instance = new KafkaUtils(TestUtilsForKafka.getConsumerProperties(), TestUtilsForKafka.getProducerProperties(), CONSUMER_TIMEOUT);
        assertNotNull(instance);

        logger.log(Level.INFO, "Initialising consumer");
        instance.initializeConsumer(topics);

        logger.log(Level.INFO, "Produce data");
        List expResult = sendAndGetMessages(50);

        logger.log(Level.INFO, "Get results from Kafka");
        List<byte[]> result = instance.getKafkaMessages();

        assertArrayEquals(expResult.toArray(), result.toArray());
        instance.closeConsumer();
    }

    private List<byte[]> sendAndGetMessages(int maxNum) throws InterruptedException, ExecutionException, TimeoutException {
        List<byte[]> ret;
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(TestUtilsForKafka.getProducerProperties("sendM-test"))) {
            ret = new ArrayList<>();
            Random r = new Random();
            InstancesHeader header = TestUtilsForKafka.generateHeader(10);
            Gson gson = new Gson();
            int i = 0;
            for (i = 0; i < maxNum; i++) {
                ProducerRecord<String, byte[]> record = new ProducerRecord(TOPIC_R, gson.toJson(TestUtilsForKafka.getData(r, 10, header)).getBytes());
                ret.add(record.value());
                producer.send(record);
            }   producer.flush();
        }
        return ret;
    }

    /**
     * Test of sendKafkaMessage method, of class KafkaUtils.
     */
    @Test
    public void testSendKafkaMessage() {
        logger.log(Level.INFO, "sendKafkaMessage");

        logger.log(Level.INFO, "Initialising producer");
        KafkaUtils instance = new KafkaUtils(TestUtilsForKafka.getConsumerProperties(), TestUtilsForKafka.getProducerProperties("rcv-test"), CONSUMER_TIMEOUT);
        instance.initializeProducer();

        logger.log(Level.INFO, "Initialising consumer");
        KafkaConsumer<String, byte[]> consumer;
        consumer = new KafkaConsumer<>(TestUtilsForKafka.getConsumerProperties());
        consumer.subscribe(Arrays.asList(TOPIC_S));

        logger.log(Level.INFO, "Produce data");
        List<byte[]> sent = new ArrayList<>();
        Random r = new Random();
        InstancesHeader header = TestUtilsForKafka.generateHeader(10);
        Gson gson = new Gson();
        for (int i = 0; i < 50; i++) {
            byte[] val = gson.toJson(TestUtilsForKafka.getData(r, 10, header)).getBytes();
            sent.add(val);
            instance.sendKafkaMessage(TOPIC_S, val);
        }

        logger.log(Level.INFO, "Get results from Kafka");
        ConsumerRecords<String, byte[]> records = consumer.poll(CONSUMER_TIMEOUT);
        Iterator<ConsumerRecord<String, byte[]>> it = records.iterator();
        List<byte[]> consumed = new ArrayList<>();
        while (it.hasNext()) {
            consumed.add(it.next().value());
        }
        consumer.close();

        assertArrayEquals(sent.toArray(), consumed.toArray());
    }

}
