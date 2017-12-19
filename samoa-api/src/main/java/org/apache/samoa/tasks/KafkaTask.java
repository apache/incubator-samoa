/*
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
package org.apache.samoa.tasks;

/*
 * #%L
 * SAMOA
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
import com.github.javacliparser.ClassOption;
import java.util.Properties;

import org.apache.samoa.topology.ComponentFactory;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javacliparser.Configurable;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.samoa.streams.kafka.KafkaDeserializer;
import org.apache.samoa.streams.kafka.KafkaDestinationProcessor;
import org.apache.samoa.streams.kafka.KafkaEntranceProcessor;
import org.apache.samoa.streams.kafka.KafkaSerializer;

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

  Properties producerProps;
  Properties consumerProps;
  int timeout;
  private KafkaDeserializer deserializer;
  private KafkaSerializer serializer;
  private String inTopic;
  private String outTopic;

  private TopologyBuilder builder;
  private Topology kafkaTopology;

  public IntOption kafkaParallelismOption = new IntOption("parallelismOption", 'p',
          "Number of destination Processors", 1, 1, Integer.MAX_VALUE);

  public IntOption timeoutOption = new IntOption("timeout", 't',
          "Kafka consumer timeout", 1, 1, Integer.MAX_VALUE);

  public StringOption inputBrokerOption = new StringOption("inputBroker", 'r', "Input brokers addresses",
          "inputTopic");

  public StringOption outputBrokerOption = new StringOption("outputBroker", 's', "Output brokers name",
          "inputTopic");

  public StringOption inputTopicOption = new StringOption("inputTopic", 'i', "Input topic name",
          "inputTopic");

  public StringOption outputTopicOption = new StringOption("outputTopic", 'o', "Output topic name",
          "outputTopic");

  public ClassOption serializerOption = new ClassOption("serializer", 'w',
          "Serializer class name",
          KafkaSerializer.class, KafkaSerializer.class.getName());

  public ClassOption deserializerOption = new ClassOption("deserializer", 'd',
          "Deserializer class name",
          KafkaDeserializer.class, KafkaDeserializer.class.getName());

  public StringOption taskNameOption = new StringOption("taskName", 'n', "Identifier of the task",
          "KafkaTask" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

  /**
   * Class constructor (for tests purposes)
   *
   * @param producerProps Properties of Kafka Producer and Consumer
   * @see <a href="http://kafka.apache.org/documentation/#producerconfigs">Kafka
   * Producer configuration</a>
   * @param consumerProps Properties of Kafka Producer and Consumer
   * @see <a href="http://kafka.apache.org/documentation/#consumerconfigs">Kafka
   * Consumer configuration</a>
   * @param inTopic Topic to which destination processor will read from
   * @param outTopic Topic to which destination processor will write into
   * @param timeout Timeout used when polling Kafka for new messages
   * @param serializer Implementation of KafkaSerializer that handles arriving
   * data serialization
   * @param deserializer Implementation of KafkaDeserializer that handles
   * arriving data deserialization
   */
  public KafkaTask(Properties producerProps, Properties consumerProps, String inTopic, String outTopic, int timeout, KafkaSerializer serializer, KafkaDeserializer deserializer) {
    this.producerProps = producerProps;
    this.consumerProps = consumerProps;
    this.deserializer = deserializer;
    this.serializer = serializer;
    this.inTopic = inTopic;
    this.outTopic = outTopic;
    this.timeout = timeout;
  }

  /**
   * Class constructor
   */
  public KafkaTask() {

  }

  @Override
  public void init() {
    producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", outputBrokerOption.getValue());

    consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", inputBrokerOption.getValue());

    serializer = serializerOption.getValue();

    deserializer = deserializerOption.getValue();

    inTopic = inputTopicOption.getValue();
    outTopic = outputTopicOption.getValue();

    timeout = timeoutOption.getValue();

    logger.info("Invoking init");
    if (builder == null) {
      builder = new TopologyBuilder();
      logger.info("Successfully instantiating TopologyBuilder");

      builder.initTopology(taskNameOption.getValue());
      logger.info("Successfully initializing SAMOA topology with name {}", taskNameOption.getValue());
    }

    // create enterance processor
    KafkaEntranceProcessor sourceProcessor = new KafkaEntranceProcessor(consumerProps, inTopic, timeout, deserializer);
    builder.addEntranceProcessor(sourceProcessor);

    // create stream
    Stream stream = builder.createStream(sourceProcessor);

    // create destination processor
    KafkaDestinationProcessor destProcessor = new KafkaDestinationProcessor(producerProps, outTopic, serializer);
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
    logger.info("Invoking setFactory: " + factory.toString());
    builder = new TopologyBuilder(factory);
    logger.info("Successfully instantiating TopologyBuilder");

    builder.initTopology(taskNameOption.getValue());
    logger.info("Successfully initializing SAMOA topology with name {}", taskNameOption.getValue());

  }

}
