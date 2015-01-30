package com.yahoo.labs.flink;


import com.yahoo.labs.flink.topology.impl.SamoaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;


public class SamoaTypeInfo extends TypeInformation {

    protected final Class tupleClass;

    public SamoaTypeInfo(Class tupleCl){
        tupleClass = tupleCl;
    }
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
    public TypeSerializer createSerializer() {
        return new SamoaTypeSerializer(tupleClass);
    }

    @Override
    public int getTotalFields() {
        return 3;
    }
}
