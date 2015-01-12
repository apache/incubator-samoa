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

import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * An implementation of SAMOA's ComponentFactory for Apache Flink
 *
 */
public class FlinkComponentFactory implements ComponentFactory{


	private final StreamExecutionEnvironment environment;

	public FlinkComponentFactory(StreamExecutionEnvironment env) {
		this.environment = env;
	}

	@Override
	public ProcessingItem createPi(Processor processor) {
		return null;
	}

	@Override
	public ProcessingItem createPi(Processor processor, int paralellism) {
		return null;
	}

	@Override
	public EntranceProcessingItem createEntrancePi(EntranceProcessor entranceProcessor) {
		return null;
	}

	@Override
	public Stream createStream(IProcessingItem sourcePi) {
		return null;
	}

	@Override
	public Topology createTopology(String topoName) {
		return null;
	}
}
