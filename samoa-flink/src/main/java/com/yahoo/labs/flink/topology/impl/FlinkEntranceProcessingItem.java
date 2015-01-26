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


import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.topology.AbstractEntranceProcessingItem;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class FlinkEntranceProcessingItem extends AbstractEntranceProcessingItem
		implements FlinkComponent, Serializable {

	private transient StreamExecutionEnvironment env;
	private transient DataStream outStream;
	private transient TypeInformation<? extends SamoaType> st;
	private static int numberOfEntrancePIs = 0;
	private int piID ;

	private ContentEvent firstEvent;

	public FlinkEntranceProcessingItem(StreamExecutionEnvironment env, EntranceProcessor proc) {
		super(proc);
		this.env = env;
		this.piID = numberOfEntrancePIs++;
	}

	@Override
	public void initialise() {
		final EntranceProcessor proc = getProcessor();
		final String streamId = getOutputStream().getStreamId();

		if (proc.hasNext()) {
			firstEvent = proc.nextEvent();

			SamoaType t = SamoaType.of(firstEvent, streamId);
			st = TypeExtractor.getForObject(t); // consider the case that there is no event...how to create an object?
		}

		outStream = env.addSource(new SourceFunction<SamoaType>() {
			EntranceProcessor entrProc = proc;
			String id = streamId;

			@Override
			public void invoke(Collector<SamoaType> collector) throws Exception {
				collector.collect(SamoaType.of(firstEvent, id));
				while (entrProc.hasNext()) {
					ContentEvent ce = entrProc.nextEvent();
					collector.collect(SamoaType.of(ce, id));
				}
			}
		}, (TypeInformation<SamoaType>) st);

		((FlinkStream) getOutputStream()).initialise();
	}


	@Override
	public boolean canBeInitialised() {
		return true;
	}

	@Override
	public boolean isInitialised() {
		return outStream != null;
	}

	@Override
	public int getId() {
		return -1; // dummy number shows that it cones from an Entrance PI
	}

	@Override
	public DataStream getOutStream() {
		return outStream;
	}
}
