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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

public class Utils {

	private static final String LOCAL_MODE = "local";

	// FLAGS
//	private static final String MODE_FLAG = "-m";
//	private static final String PARALLELISM_FLAG = "-p";

	//config values
	public static boolean isLocal;
	public static String flinkMaster;
	public static int flinkPort;
	public static String[] dependecyJars;
	public static int parallelism;

	public enum Partitioning {SHUFFLE, ALL, GROUP}

	public static TypeInformation<SamoaType> samoaTypeInformation = new SamoaTypeInfo();

	public static SamoaTypeSerializer samoaTypeSerializer = new SamoaTypeSerializer();

	public static DataStream subscribe(DataStream<SamoaType> stream, Partitioning partitioning) {
		switch (partitioning) {
			case ALL:
				return stream.broadcast();
			case GROUP:
				return stream.groupBy(new KeySelector<SamoaType, String>() {
					@Override
					public String getKey(SamoaType samoaType) throws Exception {
						return samoaType.f0;
					}
				});
			case SHUFFLE:
			default:
				return stream.shuffle();
		}
	}

	public static FilterFunction<SamoaType> getFilter(final String streamID) {
		return new FilterFunction<SamoaType>() {
			@Override
			public boolean filter(SamoaType o) throws Exception {
				return o.f2.equals(streamID);
			}
		};
	}

	public static Byte[] convert(byte[] arr) {
		Byte[] ret = new Byte[arr.length];
		for (int i = 0; i < arr.length; i++) {
			ret[i] = Byte.valueOf(arr[i]);
		}
		return ret;
	}

	public static byte[] convert(Byte[] arr) {
		byte[] ret = new byte[arr.length];
		for (int i = 0; i < arr.length; i++) {
			ret[i] = arr[i];
		}
		return ret;
	}


	public static void extractFlinkArguments(List<String> tmpargs) {
		int modePosition = tmpargs.size() - 1;

		//extract mode
		String choice = tmpargs.get(modePosition).trim();
		if (LOCAL_MODE.equals(choice)) {
			isLocal = true;
		} else {
			isLocal = false;
			//appropriate values for flinkMaster/port/jar dependencies etc
		}
		tmpargs.remove(modePosition);

		//extract parallelism
		int parallelismPosition = tmpargs.size() - 1;
		try {
			choice = tmpargs.get(parallelismPosition).trim();
			parallelism = Integer.parseInt(choice);
			tmpargs.remove(parallelismPosition);
		} catch (NumberFormatException nfe) {
			parallelism = 2;
		}

	}
}
