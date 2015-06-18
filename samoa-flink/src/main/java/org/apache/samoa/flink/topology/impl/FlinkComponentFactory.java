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


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;
import org.apache.samoa.topology.*;

/**
 * An implementation of SAMOA's ComponentFactory for Apache Flink
 */
public class FlinkComponentFactory implements ComponentFactory {

	private StreamExecutionEnvironment env;

	public FlinkComponentFactory(StreamExecutionEnvironment env) {
		this.env = env;
	}

	@Override
	public ProcessingItem createPi(Processor processor) {
		return new FlinkProcessingItem(env, processor);
	}

	@Override
	public ProcessingItem createPi(Processor processor, int parallelism) {
		return new FlinkProcessingItem(env, processor, parallelism);
	}

	@Override
	public EntranceProcessingItem createEntrancePi(EntranceProcessor entranceProcessor) {
		return new FlinkEntranceProcessingItem(env, entranceProcessor);
	}

	@Override
	public Stream createStream(IProcessingItem sourcePi) {
		if (sourcePi instanceof FlinkProcessingItem)
			return ((FlinkProcessingItem) sourcePi).createStream();
		else return new FlinkStream((FlinkComponent) sourcePi);
	}

	@Override
	public Topology createTopology(String topologyName) {
		return new FlinkTopology(topologyName, env);
	}
}
