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


import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Common interface of FlinkEntranceProcessingItem and FlinkProcessingItem
 */
public interface FlinkComponent {

	/**
	 * An initiation of the node. It should create the right invokables and apply the appropriate
	 * stream transformations
	 */
	public void initialise();

	public boolean canBeInitialised();

	public boolean isInitialised();

	public DataStream<SamoaType> getOutStream();

	public int getId();

}
