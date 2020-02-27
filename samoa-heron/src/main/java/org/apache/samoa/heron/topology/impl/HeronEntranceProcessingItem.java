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

import java.util.Map;
import java.util.UUID;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.topology.AbstractEntranceProcessingItem;
import org.apache.samoa.topology.EntranceProcessingItem;
import org.apache.samoa.topology.Stream;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * EntranceProcessingItem implementation for Heron.
 */
class HeronEntranceProcessingItem extends AbstractEntranceProcessingItem implements HeronTopologyNode {
    private final HeronEntranceSpout piSpout;

    HeronEntranceProcessingItem(EntranceProcessor processor) {
        this(processor, UUID.randomUUID().toString());
    }

    HeronEntranceProcessingItem(EntranceProcessor processor, String friendlyId) {
        super(processor);
        this.setName(friendlyId);
        this.piSpout = new HeronEntranceSpout(processor);
    }

    @Override
    public EntranceProcessingItem setOutputStream(Stream stream) {
        // piSpout.streams.add(stream);
        piSpout.setOutputStream((HeronStream) stream);
        return this;
    }

    @Override
    public Stream getOutputStream() {
        return piSpout.getOutputStream();
    }

    @Override
    public void addToTopology(HeronTopology topology, int parallelismHint) {
        topology.getHeronBuilder().setSpout(this.getName(), piSpout, parallelismHint);
    }

    @Override
    public HeronStream createStream() {
        return piSpout.createStream(this.getName());
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

    /**
     * Resulting Spout of StormEntranceProcessingItem
     */
    final static class HeronEntranceSpout extends BaseRichSpout {

        private static final long serialVersionUID = -9066409791668954099L;

        // private final Set<StormSpoutStream> streams;
        private final EntranceProcessor entranceProcessor;
        private HeronStream outputStream;

        // private transient SpoutStarter spoutStarter;
        // private transient Executor spoutExecutors;
        // private transient LinkedBlockingQueue<StormTupleInfo> tupleInfoQueue;

        private SpoutOutputCollector collector;

        HeronEntranceSpout(EntranceProcessor processor) {
            // this.streams = new HashSet<StormSpoutStream>();
            this.entranceProcessor = processor;
        }

        public HeronStream getOutputStream() {
            return outputStream;
        }

        public void setOutputStream(HeronStream stream) {
            this.outputStream = stream;
        }

        @Override
        public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            // this.tupleInfoQueue = new LinkedBlockingQueue<StormTupleInfo>();

            // Processor and this class share the same instance of stream
            // for (StormSpoutStream stream : streams) {
            // stream.setSpout(this);
            // }
            // outputStream.setSpout(this);

            this.entranceProcessor.onCreate(context.getThisTaskId());
            // this.spoutStarter = new SpoutStarter(this.starter);

            // this.spoutExecutors = Executors.newSingleThreadExecutor();
            // this.spoutExecutors.execute(spoutStarter);
        }

        @Override
        public void nextTuple() {
            if (entranceProcessor.hasNext()) {
                Values value = newValues(entranceProcessor.nextEvent());
                collector.emit(outputStream.getOutputId(), value);
            } else
                Utils.sleep(1000);
            // StormTupleInfo tupleInfo = tupleInfoQueue.poll(50,
            // TimeUnit.MILLISECONDS);
            // if (tupleInfo != null) {
            // Values value = newValues(tupleInfo.getContentEvent());
            // collector.emit(tupleInfo.getHeronStream().getOutputId(), value);
            // }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // for (HeronStream stream : streams) {
            // declarer.declareStream(stream.getOutputId(), new
            // Fields(StormSamoaUtils.CONTENT_EVENT_FIELD,
            // StormSamoaUtils.KEY_FIELD));
            // }
            declarer.declareStream(outputStream.getOutputId(), new Fields(HeronSamoaUtils.CONTENT_EVENT_FIELD,
                    HeronSamoaUtils.KEY_FIELD));
        }

        HeronStream createStream(String piId) {
            // StormSpoutStream stream = new StormSpoutStream(piId);
            HeronStream stream = new HeronBoltStream(piId);
            // streams.add(stream);
            return stream;
        }

        // void put(StormSpoutStream stream, ContentEvent contentEvent) {
        // tupleInfoQueue.add(new StormTupleInfo(stream, contentEvent));
        // }

        private Values newValues(ContentEvent contentEvent) {
            return new Values(contentEvent, contentEvent.getKey());
        }

        // private final static class StormTupleInfo {
        //
        // private final HeronStream stream;
        // private final ContentEvent event;
        //
        // StormTupleInfo(HeronStream stream, ContentEvent event) {
        // this.stream = stream;
        // this.event = event;
        // }
        //
        // public HeronStream getHeronStream() {
        // return this.stream;
        // }
        //
        // public ContentEvent getContentEvent() {
        // return this.event;
        // }
        // }

        // private final static class SpoutStarter implements Runnable {
        //
        // private final TopologyStarter topoStarter;
        //
        // SpoutStarter(TopologyStarter topoStarter) {
        // this.topoStarter = topoStarter;
        // }
        //
        // @Override
        // public void run() {
        // this.topoStarter.start();
        // }
        // }
    }
}
