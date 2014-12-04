package com.yahoo.labs.flink.topology.impl;


import com.yahoo.labs.samoa.topology.AbstractTopology;
import com.yahoo.labs.samoa.topology.IProcessingItem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A SAMOA topology on Apache Flink
 */
public class FlinkTopology extends AbstractTopology {

	private StreamExecutionEnvironment environment;

	public FlinkTopology(String name) {
		super(name);
		this.environment = StreamExecutionEnvironment.getExecutionEnvironment();
	}

	@Override
	public void addProcessingItem(IProcessingItem procItem) {
		super.addProcessingItem(procItem);
	}

	@Override
	public void addProcessingItem(IProcessingItem procItem, int parallelismHint) {
		super.addProcessingItem(procItem, parallelismHint);
	}

	public StreamExecutionEnvironment getEnvironment() {
		return environment;
	}
}
