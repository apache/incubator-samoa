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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
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
import org.apache.kafka.common.utils.Time;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.learners.InstanceContentEvent;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author pwawrzyniak <your.name at your.org>
 */
public class KafkaDestinationProcessorTest {

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private static final String TOPIC = "test-kdp";
    private static final int NUM_INSTANCES = 500;
    private static final int CONSUMER_TIMEOUT = 1000;

    private static KafkaServer kafkaServer;
    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static String zkConnect;

    public KafkaDestinationProcessorTest() {
    }

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

        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

    }

    @AfterClass
    public static void tearDownClass() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    @Before
    public void setUp() throws IOException {

    }

    @After
    public void tearDown() {

    }

    @Test
    public void testSendingData() throws InterruptedException, ExecutionException, TimeoutException {

        final Logger logger = Logger.getLogger(KafkaDestinationProcessorTest.class.getName());
        Properties props = TestUtilsForKafka.getProducerProperties();
        props.setProperty("auto.offset.reset", "earliest");
        KafkaDestinationProcessor kdp = new KafkaDestinationProcessor(props, TOPIC, new KafkaJsonMapper(Charset.defaultCharset()));
        kdp.onCreate(1);

        final int[] i = {0};
        
//         prepare new thread for data receiveing
        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(TestUtilsForKafka.getConsumerProperties());
                consumer.subscribe(Arrays.asList(TOPIC));
                while (i[0] < NUM_INSTANCES) {
                    try {
                        ConsumerRecords<String, byte[]> cr = consumer.poll(CONSUMER_TIMEOUT);

                        Iterator<ConsumerRecord<String, byte[]>> it = cr.iterator();
                        while (it.hasNext()) {
                            ConsumerRecord<String, byte[]> record = it.next();
                            logger.info(new String(record.value()));
                            logger.log(Level.INFO, "Current read offset is: {0}", record.offset());
                            i[0]++;
                        }
                        
                        Thread.sleep(1);

                    } catch (InterruptedException ex) {
                        Logger.getLogger(KafkaDestinationProcessorTest.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                consumer.close();
            }
        });
        th.start();

        int z = 0;
        Random r = new Random();
        InstancesHeader header = TestUtilsForKafka.generateHeader(10);

        for (z = 0; z < NUM_INSTANCES; z++) {
            InstanceContentEvent event = TestUtilsForKafka.getData(r, 10, header);
            kdp.process(event);
            logger.log(Level.INFO, "{0} {1}", new Object[]{"Sent item with id: ", z});
            Thread.sleep(5);
        }
        // wait for all instances to be read
        Thread.sleep(100);        
        assertEquals("Number of sent and received instances", z, i[0]);
    }
}
