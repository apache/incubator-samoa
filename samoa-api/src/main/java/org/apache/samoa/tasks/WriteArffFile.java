package org.apache.samoa.tasks;

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

import com.github.javacliparser.*;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.moa.streams.InstanceStream;
import org.apache.samoa.moa.streams.clustering.RandomRBFGeneratorEvents;
import org.apache.samoa.streams.PrequentialSourceProcessor;
import org.apache.samoa.topology.ComponentFactory;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;

/**
 * A task that runs and evaluates a distributed clustering algorithm.
 */
public class WriteArffFile implements Task, Configurable {


    private static final long serialVersionUID = -8246537378371580524L;

    private static final Logger logger = LoggerFactory.getLogger(WriteArffFile.class);

    public ClassOption streamOption = new ClassOption("stream", 's', "Input stream.", InstanceStream.class,
            RandomRBFGeneratorEvents.class.getName());

    public FileOption arffFileOption = new FileOption("arffFile", 'f',
            "Destination ARFF file.", null, "arff", true);

    public IntOption maxInstancesOption = new IntOption("maxInstances", 'm',
            "Maximum number of instances to write to file.", 10000000, 0,
            Integer.MAX_VALUE);

    public FlagOption suppressHeaderOption = new FlagOption("suppressHeader",
            'h', "Suppress header from output.");


    public void getDescription(StringBuilder sb) {
        sb.append("Writing a stream to an ARFF File");
    }

    @Override
    public void init() {

        if (builder == null) {
            logger.warn("Builder was not initialized, initializing it from the Task");

            builder = new TopologyBuilder();
            logger.debug("Successfully instantiating TopologyBuilder");

            builder.initTopology(arffFileOption.getValue());
            logger.debug("Successfully initializing SAMOA topology with name {}", arffFileOption.getValue());
        }

        InstanceStream stream = this.streamOption.getValue();
        initStream(stream);

        preqSource = new PrequentialSourceProcessor();
        preqSource.setStreamSource(stream);
        preqSource.setMaxNumInstances(this.maxInstancesOption.getValue());
        builder.addEntranceProcessor(preqSource);

        Stream sourcePiOutputStream = builder.createStream(preqSource);

        File destFile = this.arffFileOption.getFile();
        if (destFile != null) {
            try {
                Writer w = new BufferedWriter(new FileWriter(destFile));
                if (!this.suppressHeaderOption.isSet()) {
                    w.write(stream.getHeader().toStringArff());
                    w.write("\n");
                }
                int numWritten = 0;
                while ((numWritten < this.maxInstancesOption.getValue())
                        && stream.hasMoreInstances()) {
                    w.write(stream.nextInstance().getData().toString());
                    w.write("\n");
                    numWritten++;
                }
                w.close();
            } catch (Exception ex) {
                throw new RuntimeException(
                        "Failed writing to file " + destFile, ex);
            }
            this.topology = builder.build();
            return;
        }
        throw new IllegalArgumentException("No destination file to write to.");
    }

    private Topology topology;

    private TopologyBuilder builder;

    private PrequentialSourceProcessor preqSource;

    @Override
    public void setFactory(ComponentFactory factory) {
        // TODO unify this code with init() for now, it's used by S4 App
        // dynamic binding theoretically will solve this problem
        builder = new TopologyBuilder(factory);
        logger.debug("Successfully instantiated TopologyBuilder");

        builder.initTopology(arffFileOption.getValue());
        logger.debug("Successfully initialized SAMOA topology with name {}", arffFileOption.getValue());

    }

    public Topology getTopology() {
        return topology;
    }

    private void initStream(InstanceStream stream) {
        if (stream instanceof AbstractOptionHandler) {
            ((AbstractOptionHandler) (stream)).prepareForUse();
        }

    }
}
