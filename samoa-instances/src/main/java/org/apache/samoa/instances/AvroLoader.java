package org.apache.samoa.instances;

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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/**
 * Load Data from Avro Stream and parse to corresponding Dense & Parse Instances Abstract Class: Subclass this class for
 * different types of Avro Encodings
 *
 */
public abstract class AvroLoader implements Loader {

  private static final long serialVersionUID = 1L;

  /** Representation of the Avro Schema for the Instances being read. Built from the first line in the data */
  protected Schema schema = null;

  /** Meta-data of the Instance */
  protected InstanceInformation instanceInformation;

  /** List of attributes in the data as read from the schema */
  protected List<Attribute> attributes;

  /** This variable is to check if the data stored is Sparse or Dense */
  protected boolean isSparseData;

  protected int classAttribute;

  /** Datum Reader for Avro Data */
  public DatumReader<GenericRecord> datumReader = null;

  public AvroLoader(int classAttribute) {
    this.classAttribute = classAttribute;
    this.isSparseData = false;
  }

  /** Intialize Avro Schema, Meta Data, InstanceInformation from Input Avro Stream */
  public abstract void initializeSchema(InputStream inputStream);

  /** Read a single SAMOA Instance from Input Avro Stream */
  public abstract Instance readInstance();

  /**
   * Method to read Dense Instances from Avro File
   * 
   * @return Instance
   */
  protected Instance readInstanceDense(GenericRecord record)
  {
    Instance instance = new DenseInstance(this.instanceInformation.numAttributes() + 1);
    int numAttribute = 0;

    for (Attribute attribute : attributes) {
      Object value = record.get(attribute.name);

      boolean isNumeric = attributes.get(numAttribute).isNumeric();
      boolean isNominal = attributes.get(numAttribute).isNominal();

      if (isNumeric)
      {
        if (value instanceof Double)
          this.setDenseValue(instance, numAttribute, (double) value);
        else if (value instanceof Long)
          this.setDenseValue(instance, numAttribute, (long) value);
        else if (value instanceof Integer)
          this.setDenseValue(instance, numAttribute, (int) value);
        else
          throw new RuntimeException("Invalid data type in the Avro data for Numeric Type : " + attribute.name);
      }
      else if (isNominal)
      {
        double valueAttribute;

        if (!(value instanceof EnumSymbol))
          throw new RuntimeException("Invalid data type in the Avro data for Nominal Type : " + attribute.name);

        EnumSymbol enumSymbolalue = (EnumSymbol) value;

        String stringValue = enumSymbolalue.toString();

        if (("?".equals(stringValue)) || (stringValue == null)) {
          valueAttribute = Double.NaN;
        } else {
          valueAttribute = this.instanceInformation.attribute(numAttribute).indexOfValue(stringValue);
        }

        this.setDenseValue(instance, numAttribute, valueAttribute);
      }
      numAttribute++;
    }

    return (numAttribute > 0) ? instance : null;

  }

  /**
   * Sets a Dense Value in the corresponding attribute index
   * 
   * @param instance
   *          is the Instance where values will be set
   * @param numAttribute
   *          is the index of the attribute
   * @param valueAttribute
   *          is the value of the attribute for this Instance
   */

  private void setDenseValue(Instance instance, int numAttribute, double valueAttribute) {

    if (this.instanceInformation.classIndex() == numAttribute)
      instance.setClassValue(valueAttribute);
    else
      instance.setValue(numAttribute, valueAttribute);
  }

  /**
   * Method to read Sparse Instances from Avro File
   * 
   * @return Instance
   */
  protected Instance readInstanceSparse(GenericRecord record) {

    Instance instance = new SparseInstance(1.0, null);
    int numAttribute = -1;
    ArrayList<Double> attributeValues = new ArrayList<Double>();
    List<Integer> indexValues = new ArrayList<Integer>();

    for (Attribute attribute : attributes) {
      numAttribute++;
      Object value = record.get(attribute.name);

      boolean isNumeric = attributes.get(numAttribute).isNumeric();
      boolean isNominal = attributes.get(numAttribute).isNominal();

      /** If value is empty/null iterate to the next attribute. **/
      if (value == null)
        continue;

      if (isNumeric)
      {
        if (value instanceof Double) {
          Double v = (double) value;
          //if (Double.isFinite(v))
          if (!Double.isNaN(v) && !Double.isInfinite(v))
            this.setSparseValue(instance, indexValues, attributeValues, numAttribute, (double) value);
        }
        else if (value instanceof Long)
          this.setSparseValue(instance, indexValues, attributeValues, numAttribute, (long) value);
        else if (value instanceof Integer)
          this.setSparseValue(instance, indexValues, attributeValues, numAttribute, (int) value);
        else
          throw new RuntimeException(AVRO_LOADER_INVALID_TYPE_ERROR + " : " + attribute.name);
      }
      else if (isNominal)
      {
        double valueAttribute;

        if (!(value instanceof EnumSymbol))
          throw new RuntimeException(AVRO_LOADER_INVALID_TYPE_ERROR + " : " + attribute.name);

        EnumSymbol enumSymbolalue = (EnumSymbol) value;

        String stringValue = enumSymbolalue.toString();

        if (("?".equals(stringValue)) || (stringValue == null)) {
          valueAttribute = Double.NaN;
        } else {
          valueAttribute = this.instanceInformation.attribute(numAttribute).indexOfValue(stringValue);
        }

        this.setSparseValue(instance, indexValues, attributeValues, numAttribute, valueAttribute);
      }
    }

    int[] arrayIndexValues = new int[attributeValues.size()];
    double[] arrayAttributeValues = new double[attributeValues.size()];

    for (int i = 0; i < arrayIndexValues.length; i++) {
      arrayIndexValues[i] = indexValues.get(i).intValue();
      arrayAttributeValues[i] = attributeValues.get(i).doubleValue();
    }

    instance.addSparseValues(arrayIndexValues, arrayAttributeValues, this.instanceInformation.numAttributes());
    return instance;

  }

  /**
   * Sets a Sparse Value in the corresponding attribute index
   * 
   * @param instance
   *          is the Instance where values will be set
   * @param indexValues
   *          is the list of Index values
   * @param attributeValues
   *          is the list of Attribute values
   * @param numAttribute
   *          is the index of the attribute
   * @param valueAttribute
   *          is the value of the attribute for this Instance
   */
  private void setSparseValue(Instance instance, List<Integer> indexValues, List<Double> attributeValues,
      int numAttribute, double valueAttribute) {

    if (this.instanceInformation.classIndex() == numAttribute) {
      instance.setClassValue(valueAttribute);
    } else {
      indexValues.add(numAttribute);
      attributeValues.add(valueAttribute);
    }
  }

  /**
   * Builds the Meta Data of from the Avro Schema
   * 
   * @return
   */
  protected InstanceInformation getHeader() {

    String relation = schema.getName();
    attributes = new ArrayList<Attribute>();

    /** By Definition, the returned list is in the order of their positions. **/
    List<Schema.Field> fields = schema.getFields();

    for (Field field : fields) {
      Schema attributeSchema = field.schema();

      /** Currently SAMOA supports only NOMINAL & Numeric Types. **/
      if (attributeSchema.getType() == Schema.Type.ENUM)
      {
        List<String> attributeLabels = attributeSchema.getEnumSymbols();
        attributes.add(new Attribute(field.name(), attributeLabels));
      }
      else if (isNumeric(field))
        attributes.add(new Attribute(field.name()));
    }
    return new InstanceInformation(relation, attributes);
  }

  private boolean isNumeric(Field field) {
    if (field.schema().getType() == Schema.Type.DOUBLE
            || field.schema().getType() == Schema.Type.FLOAT
            || field.schema().getType() == Schema.Type.LONG
            || field.schema().getType() == Schema.Type.INT)
      return true;
    if (field.schema().getType() == Schema.Type.UNION) {
      for (Schema schema: field.schema().getTypes()) {
        if (schema.getType() == Schema.Type.DOUBLE
                || schema.getType() == Schema.Type.FLOAT
                || schema.getType() == Schema.Type.LONG
                || schema.getType() == Schema.Type.INT)
          return true;
      }
    }
    return false;
  }

  /**
   * Identifies if the dataset is is Sparse or Dense
   * 
   * @return boolean
   */
  protected boolean isSparseData()
  {
    List<Schema.Field> fields = schema.getFields();
    for (Field field : fields) {
      Schema attributeSchema = field.schema();

      /** If even one attribute has a null union (nullable attribute) consider it as sparse data **/
      if (attributeSchema.getType() == Schema.Type.UNION)
      {
        List<Schema> unionTypes = attributeSchema.getTypes();
        for (Schema unionSchema : unionTypes) {
          if (unionSchema.getType() == Schema.Type.NULL)
            return true;
        }
      }

    }
    return false;
  }

  @Override
  public InstanceInformation getStructure() {
    return this.instanceInformation;
  }

  /** Error Messages to for all types of Avro Loaders */
  protected static final String AVRO_LOADER_INVALID_TYPE_ERROR = "Invalid data type in the Avro data";
  protected static final String AVRO_LOADER_SCHEMA_READ_ERROR = "Exception while reading the schema from Avro File";
  protected static final String AVRO_LOADER_INSTANCE_READ_ERROR = "Exception while reading the Instance from Avro File.";
}
