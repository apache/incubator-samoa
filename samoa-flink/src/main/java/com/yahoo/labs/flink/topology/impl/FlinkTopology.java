package com.yahoo.labs.flink.topology.impl;

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


import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.yahoo.labs.flink.FlinkDoTask;
import com.yahoo.labs.flink.Utils;
import com.yahoo.labs.samoa.topology.AbstractTopology;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * A SAMOA topology on Apache Flink
 */
public class FlinkTopology extends AbstractTopology {

	public static StreamExecutionEnvironment env;

	public FlinkTopology(String name, StreamExecutionEnvironment env) {
		super(name);
		this.env = env;
	}

	public StreamExecutionEnvironment getEnvironment() {
		return env;
	}

	public void build() {
		for (EntranceProcessingItem src : getEntranceProcessingItems()) {
			((FlinkEntranceProcessingItem) src).initialise();
		}
		initPIs(ImmutableList.copyOf(Iterables.filter(getProcessingItems(), FlinkProcessingItem.class)));
	}

	private static void initPIs(ImmutableList<FlinkProcessingItem> flinkComponents) {
		if (flinkComponents.isEmpty()) return;

		for (FlinkProcessingItem comp : flinkComponents) {
			if (comp.canBeInitialised() && !comp.isInitialised()) {
				comp.initialise();
				comp.initialiseStreams();

			}//if component is part of a circle
			else if (comp.isPartOfCircle() && !comp.isInitialised()) {
				//check if circle can be initialized
				if (circleCanBeInitialised(comp.getCircleId())) {
					System.out.println("Circle can be initialised");
					initialiseCircle(comp.getCircleId());
				} else System.out.println("Circle cannot be initialised");
			}

		}
		initPIs(ImmutableList.copyOf(Iterables.filter(flinkComponents, new Predicate<FlinkProcessingItem>() {
			@Override
			public boolean apply(FlinkProcessingItem flinkComponent) {
				return !flinkComponent.isInitialised();
			}
		})));
	}


	private static boolean circleCanBeInitialised(int circleId) {

		List<Integer> circleIds = new ArrayList<>();
		for (FlinkProcessingItem pi : FlinkDoTask.circles.get(circleId)) {
			circleIds.add(pi.getComponentId());
		}

		//check that all incoming to the circle streams are initialised
		for (FlinkProcessingItem procItem : FlinkDoTask.circles.get(circleId)) {
			for (Tuple3<FlinkStream, Utils.Partitioning, Integer> inputStream : procItem.getInputStreams()) {
				//if a inputStream is not initialized AND source of inputStream is not in the circle
				if ((!inputStream.f0.isInitialised()) && (!circleIds.contains(inputStream.f2)))
					return false;
			}
		}
		return true;
	}

	private static void initialiseCircle(int circleId) {
		//get the head and tail of circle
		FlinkProcessingItem tail = FlinkDoTask.circles.get(circleId).get(0);
		FlinkProcessingItem head = FlinkDoTask.circles.get(circleId).get(FlinkDoTask.circles.size());

		//TODO:: refactor this part to apply in general cases also
		//initialise source stream of the iteration, so as to use it for the iteration starting point
		//head.initialiseCircleHead();
		for (Tuple3<FlinkStream, Utils.Partitioning, Integer> inputStream : head.getInputStreams()) {
			if (inputStream.f0.isInitialised()) { //if input stream is initialised
				try {
					if (head.getInStream() == null) {
						//initialise input stream of the head of the iteration
						head.setInStream(Utils.subscribe(inputStream.f0.getOutStream(), inputStream.f1));
					} else {
						//merge already initialized streams of head of the iteration
						DataStream<SamoaType> in = head.getInStream().merge(Utils.subscribe(inputStream.f0.getOutStream(), inputStream.f1));
						head.setInStream(in);
					}
				} catch (Exception e) {
					System.out.println(e);
				}
			}
		}
		IterativeDataStream ids = head.getInStream().iterate();
		DataStream temp = ids.transform("samoaProcessor", Utils.samoaTypeInformation, head).setParallelism(head.getParallelism());
		head.setOutStream(temp);
		head.initialiseStreams();


		//************tail*****************************************************************
		tail.initialise();
		tail.initialiseStreams();

		//refactor that:get(0) --> for the specific example
		ids.closeWith(tail.getOutStream().filter(Utils.getFilter(tail.getOutputStreams().get(0).getStreamId())));
	}


}
