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

package com.yahoo.labs.flink;

import com.yahoo.labs.flink.topology.impl.SamoaType;
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public final class SamoaTypeSerializer extends TypeSerializer<SamoaType> {

	private TypeSerializer<String> stringTypeSerializer = BasicTypeInfo.STRING_TYPE_INFO.createSerializer(null);
	private TypeSerializer<Byte[]> byteTypeSerializer = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO.createSerializer(null);

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<SamoaType> duplicate() {
		return this;
	}

	@Override
	public SamoaType createInstance() {
		return new SamoaType();
	}

	@Override
	public SamoaType copy(SamoaType samoaType) {
		return SamoaType.of(samoaType.f1, samoaType.f2);
	}

	@Override
	public SamoaType copy(SamoaType samoaType, SamoaType t1) {
		samoaType.setFields(t1.f0, t1.f1, t1.f2);
		return samoaType;
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
		samoaType.setField(SerializationUtils.deserialize(Utils.convert(byteTypeSerializer.deserialize(dataInputView))), 1);
		samoaType.setField(stringTypeSerializer.deserialize(dataInputView), 2);
		return samoaType;
	}

	@Override
	public void copy(DataInputView dataInputView, DataOutputView dataOutputView) throws IOException {
		serialize(deserialize(dataInputView), dataOutputView);
	}
}