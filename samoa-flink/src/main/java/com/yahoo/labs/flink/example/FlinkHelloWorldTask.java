package com.yahoo.labs.flink.example;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2015 Yahoo! Inc.
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

import com.github.javacliparser.Configurable;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
import com.yahoo.labs.samoa.tasks.Task;
import com.yahoo.labs.samoa.topology.ComponentFactory;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.Topology;
import com.yahoo.labs.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class FlinkHelloWorldTask implements Task, Configurable {

    private static final long serialVersionUID = 1L;
    private static Logger logger = LoggerFactory.getLogger(FlinkHelloWorldTask.class);

    /**
     * The topology builder for the task.
     */
    private TopologyBuilder builder;
    /**
     * The topology that will be created for the task
     */
    private Topology flinkhelloWorldTopology;

    public IntOption instanceLimitOption = new IntOption("instanceLimit", 'i',
            "Maximum number of instances to generate (-1 = no limit).", 1000000, -1, Integer.MAX_VALUE);

    public IntOption flinkhelloWorldParallelismOption = new IntOption("parallelismOption", 'p',
            "Number of destination Processors", 1, 1, Integer.MAX_VALUE);

    public StringOption evaluationNameOption = new StringOption("evaluationName", 'n',
            "Identifier of the evaluation", "HelloWorldTask" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

    @Override
    public void init() {
        // create source EntranceProcessor
        /* The event source for the topology. Implements EntranceProcessor */
        FlinkHelloWorldSourceProcessor sourceProcessor = new FlinkHelloWorldSourceProcessor(100);
        builder.addEntranceProcessor(sourceProcessor);

        // create Stream
        Stream stream = builder.createStream(sourceProcessor);

        // create destination Processor
        FlinkHelloWorldDestinationProcessor firstDestProcessor = new FlinkHelloWorldDestinationProcessor();
        builder.addProcessor(firstDestProcessor, flinkhelloWorldParallelismOption.getValue());
        builder.connectInputShuffleStream(stream, firstDestProcessor);

        Stream stream1 = builder.createStream(firstDestProcessor);
        firstDestProcessor.setOutputStream(stream1);
        //create Second Destination Processor
        FlinkHelloWorldDestinationProcessor secondDestProcessor = new FlinkHelloWorldDestinationProcessor();
        builder.addProcessor(secondDestProcessor,flinkhelloWorldParallelismOption.getValue());
        builder.connectInputShuffleStream(stream1, secondDestProcessor);

        Stream stream2 = builder.createStream(secondDestProcessor);
        secondDestProcessor.setOutputStream(stream2);

        FlinkHelloWorldDestinationProcessor thirdDestProcessor = new FlinkHelloWorldDestinationProcessor();
        builder.addProcessor(thirdDestProcessor,flinkhelloWorldParallelismOption.getValue());
        builder.connectInputShuffleStream(stream2, thirdDestProcessor);

        Stream stream3 = builder.createStream(thirdDestProcessor);
        thirdDestProcessor.setToSplit(true);
        thirdDestProcessor.setSplitStream(stream3);
        builder.connectInputShuffleStream(stream3,firstDestProcessor);

        Stream stream4 = builder.createStream(thirdDestProcessor);
        thirdDestProcessor.setOutputStream(stream4);
        FlinkHelloWorldDestinationProcessor forthDestProcessor = new FlinkHelloWorldDestinationProcessor();
        forthDestProcessor.setLastProc(true);

        builder.addProcessor(forthDestProcessor,flinkhelloWorldParallelismOption.getValue());
        builder.connectInputShuffleStream(stream4,forthDestProcessor);

        // topology till now: s -> 1-> 2 -> 3 -> 4
        //                         |________|

        //sub-topology to add in the existing one: 1-> 5

        Stream stream5 = builder.createStream(firstDestProcessor);
        firstDestProcessor.setStringOutputStream(stream5);

        FlinkHelloWorldDestinationProcessor fifthDestProcessor = new FlinkHelloWorldDestinationProcessor();
        builder.addProcessor(fifthDestProcessor);
        builder.connectInputShuffleStream(stream5,fifthDestProcessor);

//        sub-topology to add in the existing one: 1-> 5->6
//                                                 |___|
        Stream stream6 = builder.createStream(fifthDestProcessor);
        fifthDestProcessor.setToSplit(true);
        fifthDestProcessor.setSplitStream(stream6);
        builder.connectInputShuffleStream(stream6,firstDestProcessor);

        FlinkHelloWorldDestinationProcessor sixthDestProcessor = new FlinkHelloWorldDestinationProcessor();
        sixthDestProcessor.setLastProc(true);
        builder.addProcessor(sixthDestProcessor);

        Stream stream7 = builder.createStream(fifthDestProcessor);
        fifthDestProcessor.setStringOutputStream(stream7);
        builder.connectInputShuffleStream(stream7,sixthDestProcessor);


        // build the topology
        flinkhelloWorldTopology = builder.build();
        logger.debug("Successfully built the topology");
    }

    @Override
    public Topology getTopology() {
        return flinkhelloWorldTopology;
    }

    @Override
    public void setFactory(ComponentFactory factory) {
        // will be removed when dynamic binding is implemented
        builder = new TopologyBuilder(factory);
        logger.debug("Successfully instantiating TopologyBuilder");
        builder.initTopology(evaluationNameOption.getValue());
        logger.debug("Successfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());
    }
}
