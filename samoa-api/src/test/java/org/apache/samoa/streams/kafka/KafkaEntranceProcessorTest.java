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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import mockit.Mocked;
import mockit.Tested;
import mockit.Expectations;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.learners.InstanceContentEvent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import org.apache.kafka.common.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samoa.instances.Attribute;
import org.apache.samoa.instances.DenseInstance;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.moa.core.FastVector;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.streams.InstanceStream;

/**
 *
 * @author pwawrzyniak
 */
//@Ignore
public class KafkaEntranceProcessorTest {

//    @Tested
//    private KafkaEntranceProcessor kep;
    private static final String ZKHOST = "10.255.251.202"; 		//10.255.251.202
    private static final String BROKERHOST = "10.255.251.214";	//10.255.251.214
    private static final String BROKERPORT = "6667";		//6667, local: 9092
    private static final String TOPIC = "samoa_test";				//samoa_test, local: test
    private static final int NUM_INSTANCES = 50;
    
    
    private static KafkaServer kafkaServer;
    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static String zkConnect;
    

    public KafkaEntranceProcessorTest() {
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
        zkClient.close();
        zkServer.shutdown();
    }

    @Before
    public void setUp() throws IOException {

    }

    @After
    public void tearDown() {

    }

    /*@Test
    public void testFetchingNewData() throws InterruptedException, ExecutionException, TimeoutException {

        Logger logger = Logger.getLogger(KafkaEntranceProcessorTest.class.getName());
        Properties props = TestUtilsForKafka.getConsumerProperties();
        props.setProperty("auto.offset.reset", "earliest");
        KafkaEntranceProcessor kep = new KafkaEntranceProcessor(props, TOPIC, 10000, new KafkaJsonMapper(Charset.defaultCharset()));
        kep.onCreate(1);

//         prepare new thread for data producing
        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(TestUtilsForKafka.getProducerProperties());

                Random r = new Random();
                InstancesHeader header = TestUtilsForKafka.generateHeader(10);
                Gson gson = new Gson();
                int i = 0;
                for (i = 0; i < NUM_INSTANCES; i++) {
                    try {
                        ProducerRecord<String, byte[]> record = new ProducerRecord(TOPIC, gson.toJson(TestUtilsForKafka.getData(r, 10, header)).getBytes());
                        long stat = producer.send(record).get(10, TimeUnit.DAYS).offset();
                        Thread.sleep(5);
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

        int z = 0;
        while (kep.hasNext() && z < NUM_INSTANCES) {
            logger.log(Level.INFO, "{0} {1}", new Object[]{z++, kep.nextEvent().toString()});
        }       

        assertEquals("Number of sent and received instances", NUM_INSTANCES, z);        
      
     
    }*/
    
    @Test
    public void testFetchingNewDataWithAvro() throws InterruptedException, ExecutionException, TimeoutException {
        Logger logger = Logger.getLogger(KafkaEntranceProcessorTest.class.getName());
        logger.log(Level.INFO, "AVRO");
    	logger.log(Level.INFO, "testFetchingNewDataWithAvro");
        Properties props = TestUtilsForKafka.getConsumerProperties();
        props.setProperty("auto.offset.reset", "earliest");
        KafkaEntranceProcessor kep = new KafkaEntranceProcessor(props, TOPIC, 10000, new KafkaAvroMapper());
        kep.onCreate(1);

//         prepare new thread for data producing
        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(TestUtilsForKafka.getProducerProperties());

                Random r = new Random();
                InstancesHeader header = TestUtilsForKafka.generateHeader(10);
                KafkaAvroMapper avroMapper = new KafkaAvroMapper();
                int i = 0;
                for (i = 0; i < NUM_INSTANCES; i++) {
                    try {
                    	//byte[] data = avroMapper.serialize(TestUtilsForKafka.getData(r, 10, header));
                    	byte[] data = KafkaAvroMapper.avroBurrSerialize(InstanceContentEvent.class, TestUtilsForKafka.getData(r, 10, header));
                    	if(data == null)
                    		Logger.getLogger(KafkaEntranceProcessorTest.class.getName()).log(Level.INFO, "Serialize result: null ("+i+")");
                        ProducerRecord<String, byte[]> record = new ProducerRecord(TOPIC, data);
                        long stat = producer.send(record).get(10, TimeUnit.DAYS).offset();
                        Thread.sleep(5);
                        Logger.getLogger(KafkaEntranceProcessorTest.class.getName()).log(Level.INFO, "Sent avro message with ID={0} to Kafka!, offset={1}", new Object[]{i, stat});
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
        while (kep.hasNext() && z < NUM_INSTANCES) {
            logger.log(Level.INFO, "{0} {1}", new Object[]{z++, kep.nextEvent().toString()});
        }       

        assertEquals("Number of sent and received instances", NUM_INSTANCES, z);        
      
     
    }

//    private Properties getProducerProperties() {
//        Properties producerProps = new Properties();
////                props.setProperty("zookeeper.connect", zkConnect);
//        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
//        producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
////        producerProps.setProperty("group.id", "test");
//        return producerProps;
//    }
//
//    private Properties getConsumerProperties() {
//        Properties consumerProps = new Properties();
//        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
//        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
////        consumerProps.setProperty("group.id", "test");
//        consumerProps.setProperty("group.id", "group0");
//        consumerProps.setProperty("client.id", "consumer0");
//        return consumerProps;

}
