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

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;


import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import kafka.server.KafkaServer;
import kafka.zk.EmbeddedZookeeper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.streams.kafka.topology.SimpleComponentFactory;
import org.apache.samoa.streams.kafka.topology.SimpleEngine;

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
 *
 * @author Jakub Jankowski
 */
@Ignore
public class KafkaTaskTest {

    private static final String ZKHOST = "127.0.0.1";//10.255.251.202"; 		//10.255.251.202
    private static final String BROKERHOST = "127.0.0.1";//"10.255.251.214";	//10.255.251.214
    private static final String BROKERPORT = "9092";		//6667, local: 9092
    private static final String TOPIC = "samoa_test";				//samoa_test, local: test
    private static final int NUM_INSTANCES = 125922;

    private static KafkaServer kafkaServer;
    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static String zkConnect;

    @BeforeClass
    public static void setUpClass() throws IOException {
        // setup Zookeeper
//        zkServer = new EmbeddedZookeeper();
//        zkConnect = ZKHOST + ":" + "2181"; //+ zkServer.port();
//        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
//        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        /*Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);*/
        // create topic
        //AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
    }

    @AfterClass
    public static void tearDownClass() {
        //kafkaServer.shutdown(); 
//        zkClient.close();
//        zkServer.shutdown();
    }

    @Before
    public void setUp() throws IOException {

    }

    @After
    public void tearDown() {

    }

    @Test
    public void testKafkaTask() throws InterruptedException, ExecutionException, TimeoutException {
        Logger logger = Logger.getLogger(KafkaTaskTest.class.getName());
        logger.log(Level.INFO, "KafkaTask");
        Properties producerProps = TestUtilsForKafka.getProducerProperties(BROKERHOST, BROKERPORT);
        Properties consumerProps = TestUtilsForKafka.getConsumerProperties(BROKERHOST, BROKERPORT);

        KafkaTask task = new KafkaTask(producerProps, consumerProps, "kafkaTaskTest", 10000, new OosTestSerializer(), new OosTestSerializer());
        task.setFactory(new SimpleComponentFactory());
        task.init();
        SimpleEngine.submitTopology(task.getTopology());

        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(TestUtilsForKafka.getProducerProperties(BROKERHOST, BROKERPORT));

                Random r = new Random();
                InstancesHeader header = TestUtilsForKafka.generateHeader(10);
                OosTestSerializer serializer = new OosTestSerializer();
                int i = 0;
                for (i = 0; i < NUM_INSTANCES; i++) {
                    try {
                        ProducerRecord<String, byte[]> record = new ProducerRecord(TOPIC, serializer.serialize(TestUtilsForKafka.getData(r, 10, header)));
                        long stat = producer.send(record).get(10, TimeUnit.DAYS).offset();
//                        Thread.sleep(5);
                        Logger.getLogger(KafkaEntranceProcessorTest.class.getName()).log(Level.INFO, "Sent message with ID={0} to Kafka!, offset={1}", new Object[]{i, stat});
                    } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                        Logger.getLogger(KafkaEntranceProcessorTest.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                producer.flush();
                producer.close();
            }
        });
        th.start();

    }
}
