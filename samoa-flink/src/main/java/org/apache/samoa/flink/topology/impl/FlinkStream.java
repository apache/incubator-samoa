/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samoa.flink.topology.impl;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.flink.helpers.Utils;
import org.apache.samoa.topology.AbstractStream;

import java.io.Serializable;


/**
 * A stream for SAMOA based on Apache Flink's DataStream
 */
public class FlinkStream extends AbstractStream implements FlinkComponent, Serializable {

	private static int outputCounter = 0;
	private FlinkComponent sourceComponent;
	private transient DataStream<SamoaType> filteredStream;
	private String filterID;

	public FlinkStream(FlinkComponent sourcePi) {
		this.sourceComponent = sourcePi;
		setStreamId("stream-" + Integer.toString(outputCounter));
		filterID = "stream-" + Integer.toString(outputCounter);
		outputCounter++;
	}

	@Override
	public void initialise() {
		if (sourceComponent instanceof FlinkProcessingItem) {
			filteredStream = sourceComponent.getOutStream().filter(Utils.getFilter(getStreamId()))
			.setParallelism(((FlinkProcessingItem) sourceComponent).getParallelism());
		} else
			filteredStream = sourceComponent.getOutStream();
	}

	@Override
	public boolean canBeInitialised() {
		return sourceComponent.isInitialised();
	}

	@Override
	public boolean isInitialised() {
		return filteredStream != null;
	}

	@Override
	public DataStream getOutStream() {
		return filteredStream;
	}

	@Override
	public void put(ContentEvent event) {
		((FlinkProcessingItem) sourceComponent).putToStream(event, this);
	}

	@Override
	public int getComponentId() {
		return -1; //dummy number shows that it comes from a Stream
	}

	public int getSourcePiId() {
		return sourceComponent.getComponentId();
	}

	@Override
	public String getStreamId() {
		return filterID;
	}
}
