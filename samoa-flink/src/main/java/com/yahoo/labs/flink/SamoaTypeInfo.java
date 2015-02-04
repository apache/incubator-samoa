package com.yahoo.labs.flink;


import com.yahoo.labs.flink.topology.impl.SamoaType;
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
	public TypeSerializer<SamoaType> createSerializer() {
		return Utils.samoaTypeSerializer;
	}

	@Override
	public int getTotalFields() {
		return 3;
	}
}
