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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;


public class SamoaTypeInfo extends TypeInformation {

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return true;
	}

	@Override
	public int getArity() {
		return 3;
	}

	@Override
	public Class getTypeClass() {
		return SamoaType.getTupleClass(3);
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer createSerializer(ExecutionConfig executionConfig) {
		return Utils.samoaTypeSerializer;
	}
	
	@Override
	public int getTotalFields() {
		return 3;
	}
}
