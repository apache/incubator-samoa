package org.apache.samoa.instances.kafka;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2017 Apache Software Foundation
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


import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.samoa.instances.instances.DenseInstanceData;
import org.apache.samoa.instances.instances.SparseInstanceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DatumReader used to read objects built with InstanceData classes
 * @author Jakub Jankowski
 *
 * @param <T>
 */
public class SamoaDatumReader<T> extends ReflectDatumReader<T> {

    private static Logger logger = LoggerFactory.getLogger(SamoaDatumReader.class);

    public SamoaDatumReader() {
        super();
    }

    /** Construct for reading instances of a class. */
    public SamoaDatumReader(Class<T> c) {
        super(c);
    }

    /** Construct where the writer's and reader's schemas are the same. */
    public SamoaDatumReader(Schema root) {
        super(root);
    }

    /** Construct given writer's and reader's schema. */
    public SamoaDatumReader(Schema writer, Schema reader) {
        super(writer, reader);
    }

    /** Construct given writer's and reader's schema and the data model. */
    public SamoaDatumReader(Schema writer, Schema reader, ReflectData data) {
        super(writer, reader, data);
    }

    /** Construct given a {@link ReflectData}. */
    public SamoaDatumReader(ReflectData data) {
        super(data);
    }

    @Override
    /**
     * Called to read a record instance. Overridden to read InstanceData.
     */
    protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
        Object r = getData().newRecord(old, expected);
        Object state = null;

        for (Field f : in.readFieldOrder()) {
            int pos = f.pos();
            String name = f.name();
            Object oldDatum = null;
            if (r instanceof DenseInstanceData) {
                r = readDenseInstanceData(r, f, oldDatum, in, state);
            } else if (r instanceof SparseInstanceData) {
                r = readSparseInstanceData(r, f, oldDatum, in, state);
            } else
                readField(r, f, oldDatum, in, state);
        }

        return r;
    }

    private Object readDenseInstanceData(Object record, Field f, Object oldDatum, ResolvingDecoder in, Object state)
            throws IOException {
        if (f.name().equals("attributeValues")) {
            Array atributes = (Array) read(oldDatum, f.schema(), in);
            double[] atributesArr = new double[atributes.size()];
            for (int i = 0; i < atributes.size(); i++) {
                atributesArr[i] = (double) atributes.get(i);
            }
            return new DenseInstanceData(atributesArr);
        }
        return null;
    }

    private Object readSparseInstanceData(Object record, Field f, Object oldDatum, ResolvingDecoder in, Object state)
            throws IOException {
        if(f.name().equals("attributeValues")) {
            Array atributes = (Array) read(oldDatum, f.schema(), in);
            double[] atributesArr = new double[atributes.size()];
            for (int i = 0; i < atributes.size(); i++)
                atributesArr[i] = (double) atributes.get(i);
            ((SparseInstanceData)record).setAttributeValues(atributesArr);
        }
        if(f.name().equals("indexValues")) {
            Array indexValues = (Array) read(oldDatum, f.schema(), in);
            int[] indexValuesArr = new int[indexValues.size()];
            for (int i = 0; i < indexValues.size(); i++) {
                indexValuesArr[i] = (int) indexValues.get(i);
            }
            ((SparseInstanceData)record).setIndexValues(indexValuesArr);
        }
        return record;
    }

}
