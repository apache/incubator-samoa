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
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.common.utils.Time;
import org.apache.samoa.streams.kafka.topology.SimpleComponentFactory;
import org.apache.samoa.streams.kafka.topology.SimpleEngine;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

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
	
    private static final String ZKHOST = "10.255.251.202"; 		//10.255.251.202
    private static final String BROKERHOST = "10.255.251.214";	//10.255.251.214
    private static final String BROKERPORT = "6667";		//6667, local: 9092
    private static final String TOPIC = "samoa_test";				//samoa_test, local: test
    private static final int NUM_INSTANCES = 500;
    
    
    private static KafkaServer kafkaServer;
    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static String zkConnect;
    
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
    
    @Test
    public void testKafkaTask() throws InterruptedException, ExecutionException, TimeoutException {
        Logger logger = Logger.getLogger(KafkaTaskTest.class.getName());
        logger.log(Level.INFO, "KafkaTask");
        Properties producerProps = TestUtilsForKafka.getProducerProperties();
        Properties consumerProps = TestUtilsForKafka.getConsumerProperties();
    	     
        KafkaTask task = new KafkaTask(producerProps, consumerProps, "kafkaTaskTest", 10000, new KafkaJsonMapper(Charset.defaultCharset()), new KafkaJsonMapper(Charset.defaultCharset()));
        task.setFactory(new SimpleComponentFactory());
        task.init();
        SimpleEngine.submitTopology(task.getTopology());
    }
}
