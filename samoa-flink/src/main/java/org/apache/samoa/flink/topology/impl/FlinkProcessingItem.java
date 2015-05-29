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


import com.google.common.collect.Lists;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.flink.helpers.Utils;
import org.apache.samoa.topology.ProcessingItem;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.utils.PartitioningScheme;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class FlinkProcessingItem extends AbstractUdfStreamOperator<SamoaType, FlinkProcessingItem.SamoaDelegateFunction> 
		implements OneInputStreamOperator<SamoaType, SamoaType>, ProcessingItem, FlinkComponent, Serializable {

	private static final Logger logger = LoggerFactory.getLogger(FlinkProcessingItem.class);
	public static final int MAX_WAIT_TIME_MILLIS = 10000;

	private final Processor processor;
	private final transient StreamExecutionEnvironment env;
	private final SamoaDelegateFunction fun;
	private transient DataStream<SamoaType> inStream;
	private transient DataStream<SamoaType> outStream;
	private transient List<FlinkStream> outputStreams = Lists.newArrayList();
	private transient List<Tuple3<FlinkStream, PartitioningScheme, Integer>> inputStreams = Lists.newArrayList();
	private int parallelism;
	private static int numberOfPIs = 0;
	private int piID;
	private List<Integer> circleId; //check if we can refactor this
	private boolean onIteration;
	//private int circleId; //check if we can refactor this

	public FlinkProcessingItem(StreamExecutionEnvironment env, Processor proc) {
		this(env, proc, 1);
	}

	public FlinkProcessingItem(StreamExecutionEnvironment env, Processor proc, int parallelism) {
		this(env, new SamoaDelegateFunction(proc), proc, parallelism);
	}

	public FlinkProcessingItem(StreamExecutionEnvironment env, SamoaDelegateFunction fun, Processor proc, int parallelism) {
		super(fun);
		this.env = env;
		this.fun = fun;
		this.processor = proc;
		this.parallelism = parallelism;
		this.piID = numberOfPIs++;
		this.circleId = new ArrayList<Integer>() {
		}; // if size equals 0, then it is part of no circle
	}

	public Stream createStream() {
		FlinkStream generatedStream = new FlinkStream(this);
		outputStreams.add(generatedStream);
		return generatedStream;
	}

	public void putToStream(ContentEvent data, Stream targetStream) {
		output.collect(SamoaType.of(data, targetStream.getStreamId()));
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.processor.onCreate(getComponentId());
	}

	@Override
	public void initialise() {
		for (Tuple3<FlinkStream, PartitioningScheme, Integer> inputStream : inputStreams) {
			if (inputStream.f0.isInitialised()) { //if input stream is initialised
				try {
					DataStream toBeMerged = Utils.subscribe(inputStream.f0.getOutStream(), inputStream.f1);
					if (inStream == null) {
						inStream = toBeMerged;
					} else {
						inStream = inStream.union(toBeMerged);
					}
				} catch (RuntimeException e) {
					e.printStackTrace();
					System.exit(1);
				}
			}
		}

		if (onIteration) {
			inStream = inStream.iterate(MAX_WAIT_TIME_MILLIS);
		}
		outStream = inStream.transform("samoaProcessor", Utils.tempTypeInfo, this).setParallelism(parallelism);
	}

	public void initialiseStreams() {
		for (FlinkStream stream : this.getOutputStreams()) {
			stream.initialise();
		}
	}

	@Override
	public boolean canBeInitialised() {
		for (Tuple3<FlinkStream, PartitioningScheme, Integer> inputStream : inputStreams) {
			if (!inputStream.f0.isInitialised()) return false;
		}
		return true;
	}

	@Override
	public boolean isInitialised() {
		return outStream != null;
	}

	@Override
	public Processor getProcessor() {
		return processor;
	}

	@Override
	public void processElement(SamoaType samoaType) throws Exception {
		fun.processEvent(samoaType.f1);
	}

	@Override
	public ProcessingItem connectInputShuffleStream(Stream inputStream) {
		inputStreams.add(new Tuple3<>((FlinkStream) inputStream, PartitioningScheme.SHUFFLE, ((FlinkStream) inputStream).getSourcePiId()));
		return this;
	}

	@Override
	public ProcessingItem connectInputKeyStream(Stream inputStream) {
		inputStreams.add(new Tuple3<>((FlinkStream) inputStream, PartitioningScheme.GROUP_BY_KEY, ((FlinkStream) inputStream).getSourcePiId()));
		return this;
	}

	@Override
	public ProcessingItem connectInputAllStream(Stream inputStream) {
		inputStreams.add(new Tuple3<>((FlinkStream) inputStream, PartitioningScheme.BROADCAST, ((FlinkStream) inputStream).getSourcePiId()));
		return this;
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public List<FlinkStream> getOutputStreams() {
		return outputStreams;
	}

	public DataStream<SamoaType> getOutStream() {
		return this.outStream;
	}

	public void setOutStream(DataStream outStream) {
		this.outStream = outStream;
	}

	@Override
	public int getComponentId() {
		return piID;
	}

	public boolean isPartOfCircle() {
		return this.circleId.size() > 0;
	}

	public List<Integer> getCircleIds() {
		return circleId;
	}

	public void addPItoLoop(int piId) {
		this.circleId.add(piId);
	}

	public DataStream<SamoaType> getInStream() {
		return inStream;
	}

	public List<Tuple3<FlinkStream, PartitioningScheme, Integer>> getInputStreams() {
		return inputStreams;
	}

	public void setOnIteration(boolean onIteration) {
		this.onIteration = onIteration;
	}

	static class SamoaDelegateFunction implements Function, Serializable {
		private final Processor proc;

		SamoaDelegateFunction(Processor proc) {
			this.proc = proc;
		}

		public void processEvent(ContentEvent event) {
			proc.process(event);
		}
	}

	public FlinkStream getInputStreamBySourceID(int sourceID) {
		for (Tuple3<FlinkStream, PartitioningScheme, Integer> fstreams : inputStreams) {
			if (fstreams.f2 == sourceID) {
				return fstreams.f0;
			}
		}
		return null;
	}

}
