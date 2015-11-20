package org.apache.samoa.flink.topology.impl;

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


import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.samoa.flink.helpers.CycleDetection;
import org.apache.samoa.topology.AbstractTopology;
import org.apache.samoa.topology.EntranceProcessingItem;
import org.apache.samoa.utils.PartitioningScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A SAMOA topology on Apache Flink
 * <p/>
 * A Samoa-Flink Streaming Topology is DAG of ProcessingItems encapsulated within custom operators.
 * Streams are tagged and filtered in each operator's output so they can be routed to the right
 * operator respectively. Building a Flink topology from a Samoa task involves invoking all these
 * stream transformations and finally, marking and initiating cycles in the graph. We have to do that
 * since Flink only allows explicit cycles in the topology started with 'iterate()' and closed with
 * 'closeWith()'. Thus, when we build a flink topology we have to do it incrementally from the
 * sources, mark cycles and initialize them with explicit iterations.
 */
public class FlinkTopology extends AbstractTopology {

    private static final Logger logger = LoggerFactory.getLogger(FlinkTopology.class);
    public static StreamExecutionEnvironment env;
    public List<List<FlinkProcessingItem>> cycles = new ArrayList<>();
    public List<Integer> backEdges = new ArrayList<Integer>(); 

    public FlinkTopology(String name, StreamExecutionEnvironment env) {
        super(name);
        this.env = env;
    }

    public StreamExecutionEnvironment getEnvironment() {
        return env;
    }

    public void build() {
        markCycles();
        for (EntranceProcessingItem src : getEntranceProcessingItems()) {
            ((FlinkEntranceProcessingItem) src).initialise();
        }
        initComponents(ImmutableList.copyOf(Iterables.filter(getProcessingItems(), FlinkProcessingItem.class)));
    }

    private void initComponents(ImmutableList<FlinkProcessingItem> flinkComponents) {
        if (flinkComponents.isEmpty()) return;

        for (FlinkProcessingItem comp : flinkComponents) {
            if (comp.canBeInitialised() && !comp.isInitialised() && !comp.isPartOfCycle()) {
                comp.initialise();
                comp.initialiseStreams();

            }//if component is part of one or more cycle
            else if (comp.isPartOfCycle() && !comp.isInitialised()) {
                for (Integer cycle : comp.getCycleIds()) {
                    //check if cycle can be initialized
                    if (completenessCheck(cycle)) {
                        logger.debug("Cycle: " + cycle + " can be initialised");
                        initializeCycle(cycle);
                    } else {
                        logger.debug("Cycle cannot be initialised");
                    }
                }
            }
        }
        initComponents(ImmutableList.copyOf(Iterables.filter(flinkComponents, new Predicate<FlinkProcessingItem>() {
            @Override
            public boolean apply(FlinkProcessingItem flinkComponent) {
                return !flinkComponent.isInitialised();
            }
        })));
    }

    /**
     * Detects and marks all cycles and backedges needed to construct a Flink topology
     */
    private void markCycles() {
        List<FlinkProcessingItem> pis = Lists.newArrayList(Iterables.filter(getProcessingItems(), FlinkProcessingItem.class));
        List<Integer>[] graph = new List[pis.size()];
        FlinkProcessingItem[] processingItems = new FlinkProcessingItem[pis.size()];


        for (int i = 0; i < pis.size(); i++) {
            graph[i] = new ArrayList<>();
        }
        //construct the graph of the topology for the Processing Items (No entrance pi is included)
        for (FlinkProcessingItem pi : pis) {
            processingItems[pi.getComponentId()] = pi;
            for (Tuple3<FlinkStream, PartitioningScheme, Integer> is : pi.getInputStreams()) {
                if (is.f2 != -1) graph[is.f2].add(pi.getComponentId());
            }
        }
        for (int g = 0; g < graph.length; g++)
            logger.debug(graph[g].toString());

        CycleDetection detCycles = new CycleDetection();
        List<List<Integer>> graphCycles = detCycles.getCycles(graph);

        //update PIs, regarding being part of a cycle.
        for (List<Integer> c : graphCycles) {
            List<FlinkProcessingItem> cycle = new ArrayList<>();
            for (Integer it : c) {
                cycle.add(processingItems[it]);
                processingItems[it].addPItoCycle(cycles.size());
            }
            cycles.add(cycle);
            backEdges.add(cycle.get(0).getComponentId());
        }
        logger.debug("Cycles detected in the topology: " + graphCycles);
    }


    private boolean completenessCheck(int cycleId) {

        List<Integer> cycleIDs = new ArrayList<>();

        for (FlinkProcessingItem pi : cycles.get(cycleId)) {
            cycleIDs.add(pi.getComponentId());
        }
        //check that all incoming to the cycle streams are initialised
        for (FlinkProcessingItem procItem : cycles.get(cycleId)) {
            for (Tuple3<FlinkStream, PartitioningScheme, Integer> inputStream : procItem.getInputStreams()) {
                //if a inputStream is not initialized AND source of inputStream is not in the cycle or a tail of other cycle
                if ((!inputStream.f0.isInitialised()) && (!cycleIDs.contains(inputStream.f2)) && (!backEdges.contains(inputStream.f2)))
                    return false;
            }
        }
        return true;
    }

    private void initializeCycle(int cycleID) {
        //get the head and tail of cycle
        FlinkProcessingItem tail = cycles.get(cycleID).get(0);
        FlinkProcessingItem head = cycles.get(cycleID).get(cycles.get(cycleID).size() - 1);

        //initialise source stream of the iteration, so as to use it for the iteration starting point
        if (!head.isInitialised()) {
            head.setOnIteration(true);
            head.initialise();
            head.initialiseStreams();
        }

        //initialise all nodes after head
        for (int node = cycles.get(cycleID).size() - 2; node >= 0; node--) {
            FlinkProcessingItem processingItem = cycles.get(cycleID).get(node);
            processingItem.initialise();
            processingItem.initialiseStreams();
        }

        SingleOutputStreamOperator backedge = (SingleOutputStreamOperator) head.getInputStreamBySourceID(tail.getComponentId()).getOutStream();
        backedge.setParallelism(head.getParallelism());
        ((IterativeStream) head.getDataStream()).closeWith(backedge);
    }


}
