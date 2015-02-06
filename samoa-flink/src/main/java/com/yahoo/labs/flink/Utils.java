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
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
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

	public enum Partitioning {SHUFFLE, ALL, GROUP}

	public static TypeInformation<SamoaType> samoaTypeInformation = new SamoaTypeInfo();

	public static TypeSerializer<SamoaType> samoaTypeSerializer = new TypeSerializer<SamoaType>() {

		private TypeSerializer<String> stringTypeSerializer = BasicTypeInfo.STRING_TYPE_INFO.createSerializer();
		private TypeSerializer<Byte[]> byteTypeSerializer = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO.createSerializer();

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public boolean isStateful() {
			return false;
		}

		@Override
		public SamoaType createInstance() {
			return new SamoaType();
		}

		@Override
		public SamoaType copy(SamoaType samoaType) {
			return null;
		}

		@Override
		public SamoaType copy(SamoaType samoaType, SamoaType t1) {
			return null;
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(SamoaType record, DataOutputView target) throws IOException {
			stringTypeSerializer.serialize(record.f0, target);
			byteTypeSerializer.serialize(Utils.convert(SerializationUtils.serialize(record.f1)), target);
			stringTypeSerializer.serialize(record.f2, target);
		}

		@Override
		public SamoaType deserialize(DataInputView dataInputView) throws IOException {
			return deserialize(new SamoaType(), dataInputView);
		}

		@Override
		public SamoaType deserialize(SamoaType samoaType, DataInputView dataInputView) throws IOException {
			samoaType.setField(stringTypeSerializer.deserialize(dataInputView), 0);
			samoaType.setField(SerializationUtils.deserialize(convert(byteTypeSerializer.deserialize(dataInputView))), 1);
			samoaType.setField(stringTypeSerializer.deserialize(dataInputView), 2);
			return samoaType;
		}

		@Override
		public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
			serialize(deserialize(dataInputView), dataOutputView);
		}
	};

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
