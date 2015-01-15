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
import com.google.common.collect.Lists;
import com.yahoo.labs.samoa.topology.AbstractTopology;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

		for (FlinkComponent comp : flinkComponents) {
			if (comp.canBeInitialised()) {
				comp.initialise();
				SplitDataStream outStream = ((SingleOutputStreamOperator) comp.getOutStream())
						.split(new OutputSelector<SamoaType>() {
							@Override
							public Iterable<String> select(SamoaType samoaType) {
								return Lists.newArrayList(samoaType.f2);
							}
						});
				((FlinkProcessingItem) comp).setOutStream(outStream);

				for (FlinkStream stream : ((FlinkProcessingItem) comp).getOutputStreams()) {
					stream.initialise();
				}
			}
		}
		initPIs(ImmutableList.copyOf(Iterables.filter(flinkComponents, new Predicate<FlinkProcessingItem>() {
			@Override
			public boolean apply(FlinkProcessingItem flinkComponent) {
				return !flinkComponent.isInitialised();
			}
		})));
	}

}
