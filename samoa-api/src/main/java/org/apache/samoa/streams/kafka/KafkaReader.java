package org.apache.samoa.streams.kafka;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2015 Apache Software Foundation
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

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

public class KafkaReader {

    protected long readOffset;

    private List<String> m_replicaBrokers = new ArrayList<String>();
    private static final int kafkaConnectionTimeOut = 100000;
    private static final int kafkaBufferSize = 64 * 1024;
    private static final int fetchSizeBytes = 100000;
    private static final int readErrorThreshold = 5;
    private static final int numLeaderCheckAttempts = 3;
    private static final int delayLeaderCheckAttempts = 100;
    private static final String leaderClientID = "leaderLookup";

    private static final Logger logger = LoggerFactory
            .getLogger(KafkaReader.class);

    public KafkaReader() {
        m_replicaBrokers = new ArrayList<String>();
        readOffset = 0L;
    }

    public ArrayList<String> run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port) {
        String message = "";
        ArrayList<String> returnInstances = new ArrayList<String>();
        PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
        if (metadata == null) {
            throw new IllegalArgumentException("Can't find metadata for Topic and Partition. Exiting");
        }
        if (metadata.leader() == null) {
            throw new IllegalArgumentException("Can't find Leader for Topic and Partition. Exiting");
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + a_topic + "_" + a_partition;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, kafkaConnectionTimeOut, kafkaBufferSize, clientName);
        int numErrors = 0;
        while (a_maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, a_port, kafkaConnectionTimeOut, kafkaBufferSize, clientName);
            }
            // reading data
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(a_topic, a_partition, readOffset, fetchSizeBytes)
                    .build();
            FetchResponse fetchResponse = null;

            try {
                fetchResponse = consumer.fetch(req);
            } catch (Exception e) {
                logger.error("ERROR occoured during fetching data from Kafka");
            }

            /**
             *  SimpleConsumer does not handle lead broker failures, you have to handle it
             *  once the fetch returns an error, we log the reason, close the consumer then try to figure
             *  out who the new leader is
             */
            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(a_topic, a_partition);
                logger.error("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > readErrorThreshold) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    continue;
                }
                consumer.close();
                consumer = null;
                try {
                    leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                continue;
            }

            // Reading data cont.
            numErrors = 0;
            Iterator it = (Iterator) fetchResponse.messageSet(a_topic, a_partition).iterator();
            MessageAndOffset messageAndOffset = null;

            try {
                messageAndOffset = (MessageAndOffset) it.next();
            } catch (Exception e) {
                logger.error("No more messages to read from Kafka.");
                return null;
            }

            long currentOffset = messageAndOffset.offset();
            if (currentOffset < readOffset) {
                logger.error("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                continue;
            }
            readOffset = messageAndOffset.nextOffset();
            ByteBuffer payload = messageAndOffset.message().payload();

            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            try {
                message = String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8");
                returnInstances.add(message);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            a_maxReads--;
        }
        if (consumer != null) consumer.close();
        return returnInstances;
    }

    /**
     * Defines where to start reading data from
     * Helpers Available:
     * kafka.api.OffsetRequest.EarliestTime() => finds the beginning of the data in the logs and starts streaming
     * from there
     * kafka.api.OffsetRequest.LatestTime()   => will only stream new messages
     *
     * @param consumer
     * @param topic
     * @param partition
     * @param whichTime
     * @param clientName
     * @return
     */
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            logger.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    /**
     * Uses the findLeader() logic we defined to find the new leader, except here we only try to connect to one of the
     * replicas for the topic/partition. This way if we can’t reach any of the Brokers with the data we are interested
     * in we give up and exit hard.
     *
     * @param a_oldLeader
     * @param a_topic
     * @param a_partition
     * @param a_port
     * @return
     * @throws Exception
     */
    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < numLeaderCheckAttempts; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(delayLeaderCheckAttempts);
                } catch (InterruptedException ie) {
                }
            }
        }
        logger.error("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    /**
     * Query a live broker to find out leader information and replica information for a given topic and partition
     *
     * @param a_seedBrokers
     * @param a_port
     * @param a_topic
     * @param a_partition
     * @return
     */
    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, kafkaConnectionTimeOut, kafkaBufferSize, leaderClientID);
                List<String> topics = new ArrayList<String>();
                topics.add(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                //call to topicsMetadata() asks the Broker you are connected to for all the details about the topic we are interested in
                List<TopicMetadata> metaData = resp.topicsMetadata();
                //loop on partitionsMetadata iterates through all the partitions until we find the one we want.
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        // add replica broker info to m_replicaBrokers
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }

}
