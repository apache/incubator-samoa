package com.yahoo.labs.flink.topology.impl;

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
