package org.apache.samoa.heron.topology.impl;

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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.topology.AbstractProcessingItem;
import org.apache.samoa.topology.ProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.heron.topology.impl.HeronStream.InputStreamId;
import org.apache.samoa.utils.PartitioningScheme;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.BoltDeclarer;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

/**
 * ProcessingItem implementation for Heron.
 *
 * @author Arinto Murdopo
 */
public class HeronProcessingItem extends AbstractProcessingItem implements HeronTopologyNode {
    private final ProcessingItemBolt piBolt;
    private BoltDeclarer piBoltDeclarer;

    // TODO: should we put parallelism hint here?
    // imo, parallelism hint only declared when we add this PI in the topology
    // open for dicussion :p

    HeronProcessingItem(Processor processor, int parallelismHint) {
        this(processor, UUID.randomUUID().toString(), parallelismHint);
    }

    HeronProcessingItem(Processor processor, String friendlyId, int parallelismHint) {
        super(processor, parallelismHint);
        this.piBolt = new ProcessingItemBolt(processor);
        this.setName(friendlyId);
    }

    @Override
    protected ProcessingItem addInputStream(Stream inputStream, PartitioningScheme scheme) {
        HeronStream stormInputStream = (HeronStream) inputStream;
        InputStreamId inputId = stormInputStream.getInputId();

        switch (scheme) {
            case SHUFFLE:
                piBoltDeclarer.shuffleGrouping(inputId.getComponentId(), inputId.getStreamId());
                break;
            case GROUP_BY_KEY:
                piBoltDeclarer.fieldsGrouping(
                        inputId.getComponentId(),
                        inputId.getStreamId(),
                        new Fields(HeronSamoaUtils.KEY_FIELD));
                break;
            case BROADCAST:
                piBoltDeclarer.allGrouping(
                        inputId.getComponentId(),
                        inputId.getStreamId());
                break;
        }
        return this;
    }

    @Override
    public void addToTopology(HeronTopology topology, int parallelismHint) {
        if (piBoltDeclarer != null) {
            // throw exception that one PI only belong to one topology
        } else {
            TopologyBuilder heronBuilder = topology.getHeronBuilder();
            this.piBoltDeclarer = heronBuilder.setBolt(this.getName(),
                    this.piBolt, parallelismHint);
        }
    }

    @Override
    public HeronStream createStream() {
        return piBolt.createStream(this.getName());
    }

    @Override
    public String getId() {
        return this.getName();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.insert(0, String.format("id: %s, ", this.getName()));
        return sb.toString();
    }

    private final static class ProcessingItemBolt extends BaseRichBolt {

        private static final long serialVersionUID = -6637673741263199198L;

        private final Set<HeronBoltStream> streams;
        private final Processor processor;

        private OutputCollector collector;

        ProcessingItemBolt(Processor processor) {
            this.streams = new HashSet<HeronBoltStream>();
            this.processor = processor;
        }

        @Override
        public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
                            OutputCollector collector) {
            this.collector = collector;
            // Processor and this class share the same instance of stream
            for (HeronBoltStream stream : streams) {
                stream.setCollector(this.collector);
            }

            this.processor.onCreate(context.getThisTaskId());
        }

        @Override
        public void execute(Tuple input) {
            Object sentObject = input.getValue(0);
            ContentEvent sentEvent = (ContentEvent) sentObject;
            processor.process(sentEvent);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            for (HeronStream stream : streams) {
                declarer.declareStream(stream.getOutputId(),
                        new Fields(HeronSamoaUtils.CONTENT_EVENT_FIELD,
                                HeronSamoaUtils.KEY_FIELD));
            }
        }

        HeronStream createStream(String piId) {
            HeronBoltStream stream = new HeronBoltStream(piId);
            streams.add(stream);
            return stream;
        }
    }
}
