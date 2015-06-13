package org.apache.samoa.flink.helpers;

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



import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.flink.topology.impl.SamoaType;
import org.apache.samoa.utils.PartitioningScheme;

import java.util.List;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

public class Utils {

	public static TypeInformation<SamoaType> tempTypeInfo = new TupleTypeInfo(SamoaType.class, STRING_TYPE_INFO, TypeExtractor.getForClass(ContentEvent.class), STRING_TYPE_INFO);

	public static DataStream subscribe(DataStream<SamoaType> stream, PartitioningScheme partitioning) {
		switch (partitioning) {
			case BROADCAST:
				return stream.broadcast();
			case GROUP_BY_KEY:
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

}
