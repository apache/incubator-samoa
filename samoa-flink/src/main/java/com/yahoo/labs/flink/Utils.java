package com.yahoo.labs.flink;

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


import com.yahoo.labs.flink.topology.impl.SamoaType;
import com.yahoo.labs.samoa.core.ContentEvent;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

public class Utils {

	private static final String LOCAL_MODE = "local";

	// FLAGS
	private static final String MODE_FLAG = "-m";
	private static final String PARALLELISM_FLAG = "-p";

	//config values
	public static boolean isLocal = true;
	public static String flinkMaster;
	public static int flinkPort;
	public static String[] dependecyJars;
	public static int parallelism = 2;
//	public static TypeInformation[] ti= {BasicTypeInfo.STRING_TYPE_INFO, TypeExtractor.
//			getForClass(ContentEvent.class), BasicTypeInfo.STRING_TYPE_INFO};

	public static TypeInformation samoaTypeInfo = new SamoaTypeInfo((new SamoaType()).getClass());

	public enum Partitioning {SHUFFLE, ALL, GROUP}

	public static DataStream subscribe(DataStream<SamoaType> stream, Partitioning partitioning) {
		switch (partitioning) {
			case ALL:
				return stream.broadcast();
			case GROUP:
				return stream.groupBy(0);
			case SHUFFLE:
			default:
				return stream.shuffle();
		}
	}

	public static void extractFlinkArguments(List<String> tmpargs) {
/*		for (int i = 1; i < tmpargs.size() - 1; i = i + 2) {
			String choice = tmpargs.get(i).trim();
			String value = tmpargs.get(i + 1).trim();
			switch (choice) {
				case PARALLELISM_FLAG:
					parallelism = Integer.valueOf(value);
					break;
				case MODE_FLAG:
					if (!(LOCAL_MODE.equals(value))) isLocal = false;
					break;
				case "-i":
					//TODO::refactor to take into consideration all possible arguments.
			}
		}*/
	}
}
