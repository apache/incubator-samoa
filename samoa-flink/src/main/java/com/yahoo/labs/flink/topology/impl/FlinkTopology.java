package com.yahoo.labs.flink.topology.impl;

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
import com.yahoo.labs.flink.com.yahoo.labs.flink.helpers.CircleDetection;
import com.yahoo.labs.flink.com.yahoo.labs.flink.helpers.Utils;
import com.yahoo.labs.samoa.topology.AbstractTopology;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import com.yahoo.labs.samoa.utils.PartitioningScheme;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A SAMOA topology on Apache Flink
 * 
 * A Samoa-Flink Streaming Topology is DAG of ProcessingItems encapsulated within custom operators.
 * Streams are tagged and filtered in each operator's output so they can be routed to the right 
 * operator respectively. Building a Flink topology from a Samoa task involves invoking all these
 * stream transformations and finally, marking and initiating loops in the graph. We have to do that
 * since Flink only allows explicit loops in the topology started with 'iterate()' and closed with
 * 'closeWith()'. Thus, when we build a flink topology we have to do it incrementally from the 
 * sources, mark loops and initialize them with explicit iterations.
 * 
 */
public class FlinkTopology extends AbstractTopology {

	private static final Logger logger = LoggerFactory.getLogger(FlinkTopology.class);
	public static StreamExecutionEnvironment env;
	public List<List<FlinkProcessingItem>> topologyLoops = new ArrayList<>();
	public  List<Integer> backEdges = new ArrayList<Integer>();

	public FlinkTopology(String name, StreamExecutionEnvironment env) {
		super(name);
		this.env = env;
	}

	public StreamExecutionEnvironment getEnvironment() {
		return env;
	}
	
	public void build() {
		markCircles();
		for (EntranceProcessingItem src : getEntranceProcessingItems()) {
			((FlinkEntranceProcessingItem) src).initialise();
		}
		initComponents(ImmutableList.copyOf(Iterables.filter(getProcessingItems(), FlinkProcessingItem.class)));
	}

	private void initComponents(ImmutableList<FlinkProcessingItem> flinkComponents) {
		if (flinkComponents.isEmpty()) return;

		for (FlinkProcessingItem comp : flinkComponents) {
			if (comp.canBeInitialised() && !comp.isInitialised() && !comp.isPartOfCircle()) {
				comp.initialise();
				comp.initialiseStreams();

			}//if component is part of one or more circle
			else if (comp.isPartOfCircle() && !comp.isInitialised()) {
				for (Integer circle : comp.getCircleIds()) {
					//check if circle can be initialized
					if (checkCircleReady(circle)) {
						logger.debug("Circle: " + circle + " can be initialised");
						initialiseCircle(circle);
					} else {
						logger.debug("Circle cannot be initialised");
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

	private void markCircles(){
		List<FlinkProcessingItem> pis = Lists.newArrayList(Iterables.filter(getProcessingItems(), FlinkProcessingItem.class));
		List<Integer>[] graph = new List[pis.size()];
		FlinkProcessingItem[] processingItems = new FlinkProcessingItem[pis.size()];


		for (int i=0;i<pis.size();i++) {
			graph[i] = new ArrayList<Integer>();
		}
		//construct the graph of the topology for the Processing Items (No entrance pi is included)
		for (FlinkProcessingItem pi: pis) {
			processingItems[pi.getComponentId()] = pi;
			for (Tuple3<FlinkStream, PartitioningScheme, Integer> is : pi.getInputStreams()) {
				if (is.f2 != -1) graph[is.f2].add(pi.getComponentId());
			}
		}
		for (int g=0;g<graph.length;g++)
			logger.debug(graph[g].toString());

		CircleDetection detCircles = new CircleDetection();
		List<List<Integer>> circles = detCircles.getCircles(graph);

		//update PIs, regarding being part of a circle.
		for (List<Integer> c : circles){
			List<FlinkProcessingItem> circle = new ArrayList<>();
			for (Integer it : c){
				circle.add(processingItems[it]);
				processingItems[it].addPItoLoop(topologyLoops.size());
			}
			topologyLoops.add(circle);
			backEdges.add(circle.get(0).getComponentId());
		}
		logger.debug("Circles detected in the topology: " + circles);
	}
	

	private boolean checkCircleReady(int circleId) {

		List<Integer> circleIds = new ArrayList<>();

		for (FlinkProcessingItem pi : topologyLoops.get(circleId)) {
			circleIds.add(pi.getComponentId());
		}
		//check that all incoming to the circle streams are initialised
		for (FlinkProcessingItem procItem : topologyLoops.get(circleId)) {
			for (Tuple3<FlinkStream, PartitioningScheme, Integer> inputStream : procItem.getInputStreams()) {
				//if a inputStream is not initialized AND source of inputStream is not in the circle or a tail of other circle
				if ((!inputStream.f0.isInitialised()) && (!circleIds.contains(inputStream.f2)) && (!backEdges.contains(inputStream.f2)))
					return false;
			}
		}
		return true;
	}

	private void initialiseCircle(int circleId) {
		//get the head and tail of circle
		FlinkProcessingItem tail = topologyLoops.get(circleId).get(0);
		FlinkProcessingItem head = topologyLoops.get(circleId).get(topologyLoops.get(circleId).size() - 1);

		//initialise source stream of the iteration, so as to use it for the iteration starting point
		if (!head.isInitialised()) {
			head.setOnIteration(true);
			head.initialise();
			head.initialiseStreams();
		}

		//initialise all nodes after head
		for (int node = topologyLoops.get(circleId).size() - 2; node >= 0; node--) {
			topologyLoops.get(circleId).get(node).initialise();
			topologyLoops.get(circleId).get(node).initialiseStreams();
		}

		((IterativeDataStream) head.getInStream()).closeWith(head.getInputStreamBySourceID(tail.getComponentId()).getOutStream());
	}


}
