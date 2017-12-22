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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.samoa.instances.instances.Instance;
import org.apache.samoa.instances.instances.InstancesHeader;
import org.apache.samoa.instances.kafka.KafkaConsumerThread;
import org.apache.samoa.instances.kafka.KafkaDeserializer;
import org.apache.samoa.instances.kafka.KafkaJsonMapper;
import org.apache.samoa.instances.loaders.KafkaLoader;
import org.apache.samoa.learners.InstanceContentEvent;
import org.junit.*;
import org.mortbay.log.Log;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;

public class KafkaLoaderTest {

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC_OOS = "samoa_test-oos";
    private static final int NUM_INSTANCES = 11111;

    private static KafkaServer kafkaServer;
    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static String zkConnect;
    private static final int TIMEOUT = 1000;

    @BeforeClass
    public static void setUpClass() throws IOException {
        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        // create topics
        AdminUtils.createTopic(zkUtils, TOPIC_OOS, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

    }

    @AfterClass
    public static void tearDownClass() {
        try {
            kafkaServer.shutdown();
            zkClient.close();
            zkServer.shutdown();
        } catch (Exception ex) {
            Logger.getLogger(KafkaLoaderTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Before
    public void setUp() throws IOException {

    }

    @After
    public void tearDown() {

    }

    @Test
    public void testFetchingNewData() throws InterruptedException, ExecutionException, TimeoutException {

        final Logger logger = Logger.getLogger(KafkaLoaderTest.class.getName());
        logger.log(Level.INFO, "OOS");
        logger.log(Level.INFO, "testFetchingNewData");
        Properties props = TestUtilsForKafka.getConsumerProperties(BROKERHOST, BROKERPORT);
        props.setProperty("auto.offset.reset", "earliest");
        KafkaLoader kafkaLoader = new KafkaLoader();

        KafkaDeserializer<Instance> kafkaDeserializer = new KafkaJsonMapper(Charset.defaultCharset());
        kafkaLoader.setDeserializer(kafkaDeserializer);

        kafkaLoader.setKafkaConsumerThread(new KafkaConsumerThread(props, Arrays.asList(TOPIC_OOS), TIMEOUT));
        kafkaLoader.runKafkaConsumerThread();

        // prepare new thread for data producing
        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(TestUtilsForKafka.getProducerProperties(BROKERHOST, BROKERPORT));

                Random r = new Random();
                InstancesHeader header = TestUtilsForKafka.generateHeader(10);

                KafkaJsonMapper kafkaJsonMapper = new KafkaJsonMapper(Charset.forName("UTF-8"));
                int i = 0;
                for (i = 0; i < NUM_INSTANCES; i++) {
                    try {
                        InstanceContentEvent event = TestUtilsForKafka.getData(r, 10, header);

                        ProducerRecord<String, byte[]> record = new ProducerRecord(TOPIC_OOS, kafkaJsonMapper.serialize(event.getInstance()));
                        long stat = producer.send(record).get(10, TimeUnit.SECONDS).offset();
                    } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                        Logger.getLogger(KafkaEntranceProcessorTest.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                producer.flush();
                producer.close();
            }
        });
        th.start();



        int z = 0;
        while (z < NUM_INSTANCES && kafkaLoader.hasNext()) {
            Instance event = kafkaLoader.readInstance();
            z++;
        }
        kafkaLoader.close();

        assertEquals("Number of sent and received instances", NUM_INSTANCES, z);

    }
}