package com.yahoo.labs.flink;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.yahoo.labs.flink.topology.impl.SamoaType;
import com.yahoo.labs.samoa.core.ContentEvent;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.NullFieldException;

import javax.swing.text.AbstractDocument;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;


public final class SamoaTypeSerializer<T extends Tuple3> extends TypeSerializer<T> {

    private static final long serialVersionUID = 1L;
    protected final Class<T> tupleClass;
    protected final int arity;

    public SamoaTypeSerializer(Class<T> tupleClass) {
        this.tupleClass = tupleClass;
        arity = 3;
    }


    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public boolean isStateful() {
        return false;
    }

    @Override
    public T createInstance() {
        return null;
    }

    @Override
    public T copy(T from) {
        return null;
    }

    @Override
    public T copy(T from, T reuse) {
        return null;
    }

    @Override
    public int getLength() {
        return -1;
    }

    public int getArity() {
        return arity;
    }

    public Class<T> getTupleClass() {
        return tupleClass;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {

        StringSerializer.INSTANCE.serialize((String)record.getField(0),target);

        Serializable s = (ContentEvent)record.getField(1);
        Byte[] temp = (Byte[]) s;

        TypeSerializer<Byte[]> bs = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO.createSerializer();
        bs.serialize(temp, target);

        StringSerializer.INSTANCE.serialize((String)record.getField(2),target);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        T tuple = instantiateRaw();
        tuple.setField(StringSerializer.INSTANCE.deserialize(source), 0);

        TypeSerializer<Byte[]> bs = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO.createSerializer();
        ContentEvent ce = (ContentEvent)((Serializable)bs.deserialize(source));
        tuple.setField(ce,1); /////////fix that

        tuple.setField(StringSerializer.INSTANCE.deserialize(source), 2);
        return tuple;
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        String field = StringSerializer.INSTANCE.deserialize((String)reuse.getField(0), source);
        reuse.setField(field, 0);

        TypeSerializer<Byte[]> bs = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO.createSerializer();
        ContentEvent ce = (ContentEvent)((Serializable)bs.deserialize(source));
        reuse.setField(ce,1); /////////fix that


        field = StringSerializer.INSTANCE.deserialize((String)reuse.getField(2), source);
        reuse.setField(field, 2);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {

    }
    private T instantiateRaw() {
        try {
            return tupleClass.newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot instantiate tuple.", e);
        }
    }

}
