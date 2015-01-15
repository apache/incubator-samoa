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
import org.apache.flink.api.java.tuple.Tuple3;

public class SamoaType extends Tuple3<String, ContentEvent, String> {
	public SamoaType() {
		super();
	}

	private SamoaType(String key, ContentEvent event, String streamId) {
		super(key, event, streamId);
	}

	public static SamoaType of(ContentEvent event, String streamId) {
		String key = event.getKey() == null ? "none" : event.getKey();
		return new SamoaType(key, event, streamId);
	}
}