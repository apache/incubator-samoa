---
title: Executing Apache SAMOA with Apache Avro Files
layout: documentation
documentation: true
---
In this tutorial page we describe how to execute SAMOA with data files in Apache Avro file format. Here is an outline of this tutorial

1. Overview of Apache Avro
2. Avro Input Format for SAMOA
3. SAMOA task execution with Avro
4. Sample Avro Data for SAMOA


### Overview of Apache Avro

Users of Apache SAMOA can now use Binary/JSON encoded Avro data as an alternate to the default ARFF file format as the data source. Avro is a remote procedure call and data serialization framework developed within Apache's Hadoop project. It uses JSON for defining data types and protocols, and serializes data in a compact binary format. Avro specifies two serialization encodings for the data: Binary and JSON, default being Binary. However the meta-data is always in JSON. Avro data is always serialized with its schema. Files that store Avro data should also include the schema for that data in the same file. 

You can find the latest Apache Avro documentation [here](https://avro.apache.org/docs/current/) for more details.

### Avro Input Format for SAMOA

It is required that the input Avro files to the SAMOA framework follow certain Input Format Rules to seamlessly work with the SAMOA Instances. The first line of Avro Source file for SAMOA (irrespective of whether data is encoded in binary or JSON) will be the metadata (schema). The data would be by default one record per line following the schema and will be mapped into 1 SAMOA instance per record.


1. Avro Primitive Types & Enums are allowed for the data as is. 
2. Avro Complex-types (e.g maps/arrays) may not be used with the exception of enum & union. I.e. no sub-structure will be allowed
3. Avro Enums may be used to represent nominal attributes.
```
E.g  
{"name":"species","type":{"type":"enum","name":"Labels","symbols":["setosa","versicolor","virginica"]}}
```

4. Avro unions may be used to represent nullability of value. However unions may not be used for different data types. 
```
E.g  
{"name":"attribute1","type":["null","int"]}  - Allowed to denote that value for attribute1 is optional
{"name":" attribute2","type":["string","int"]}  – Not allowed
```

5. Label (if any) would be the last attribute.
6. Timestamps are not supported as of now within SAMOA.



### SAMOA task execution with Avro

You may execute a SAMOA task using the aforementioned `bin/samoa` script with the following format: `bin/samoa <platform> <jar> "<task>"`.
Follow this [link](Executing-SAMOA-with-Apache-S4)  and this [link](Executing-SAMOA-with-Apache-Storm) to learn more about deploying SAMOA on Apache S4 and Apache Storm respectively. The Avro files can be used as data sources for any of the aforementioned platforms. The only addition that needs to be made in the commands is as follows:  `AvroFileStream <file_name> -e <file_format>` . Examples are given below for different modes

#### Local - Avro JSON
```
bin/samoa local target/SAMOA-Local-0.4.0-incubating-SNAPSHOT.jar "PrequentialEvaluation -l classifiers.ensemble.Bagging -s (AvroFileStream -f covtypeNorm_json.avro -e json) -f 100000"
```

#### Local - Avro Binary
```
bin/samoa local target/SAMOA-Local-0.4.0-incubating-SNAPSHOT.jar "PrequentialEvaluation -l classifiers.ensemble.Bagging -s (AvroFileStream -f covtypeNorm_binary.avro -e binary) -f 100000"
```
#### Storm - Avro JSON
```
bin/samoa storm target/SAMOA-Storm-0.4.0-incubating-SNAPSHOT.jar "PrequentialEvaluation -l classifiers.ensemble.Bagging -s (AvroFileStream -f covtypeNorm_json.avro -e json) -f 100000"
```
#### Storm - Avro Binary
```
bin/samoa storm target/SAMOA-Storm-0.4.0-incubating-SNAPSHOT.jar "PrequentialEvaluation -l classifiers.ensemble.Bagging -s (AvroFileStream -f covtypeNorm_binary.avro -e binary) -f 100000"
```


### Sample Avro Data for SAMOA

The samples below describes how the default ARFF file formats may be converted to JSON/Binary Encoded Avro formats.

#### Iris Dataset - Default ARFF Format

```
@RELATION iris  
@ATTRIBUTE sepallength  NUMERIC  
@ATTRIBUTE sepalwidth   NUMERIC     
@ATTRIBUTE petallength  NUMERIC     
@ATTRIBUTE petalwidth   NUMERIC     
@ATTRIBUTE class  {setosa,versicolor,virginica}    
@DATA  
5.1,3.5,1.4,0.2,setosa     
4.9,3.0,1.4,0.2,virginica      
4.7,3.2,1.3,0.2,virginica     
4.6,3.1,1.5,0.2,setosa  
```


#### Iris Dataset - JSON Encoded AVRO Format

```
{"type":"record","name":"Iris","namespace":"com.yahoo.labs.samoa.avro.iris","fields":[{"name":"sepallength","type":"double"},{"name":"sepalwidth","type":"double"},{"name":"petallength","type":"double"},{"name":"petalwidth","type":"double"},{"name":"class","type":{"type":"enum","name":"Labels","symbols":["setosa","versicolor","virginica"]}}]}  
{"sepallength":5.1,"sepalwidth":3.5,"petallength":1.4,"petalwidth":0.2,"class":"setosa"}  
{"sepallength":3.0,"sepalwidth":1.4,"petallength":4.9,"petalwidth":0.2,"class":"virginica"}  
{"sepallength":4.7,"sepalwidth":3.2,"petallength":1.3,"petalwidth":0.2,"class":"virginica"}  
{"sepallength":3.1,"sepalwidth":1.5,"petallength":4.6,"petalwidth":0.2,"class":"setosa"}  
```

#### Iris Dataset - Binary Encoded AVRO Format

```
Objavro.schema΅{"type":"record","name":"Iris","namespace":"com.yahoo.labs.samoa.avro.iris","fields":[{"name":"sepallength","type":"double"},{"name":"sepalwidth","type":"double"},{"name":"petallength","type":"double"},{"name":"petalwidth","type":"double"},{"name":"class","type":{"type":"enum","name":"Labels","symbols":["setosa","versicolor","virginica"]}}]} !<khCrֱS빧ީȂffffff@      @ffffffٙٙɿ       @ffffffٙٙ@ڙٙٙɿΌ͌͌@ڙٙٙ	@Ό͌͌ٙٙɿΌ͌͌@      𿦦ffff@ڙٙٙɿ !<khCrֱS빧ީ
```

#### Forest CoverType Dataset 
The JSON & Binary encoded AVRO Files covtypeNorm\_json.avro & covtypeNorm\_binary.avro for the Forest CoverType dataset can be found at [Wiki](https://cwiki.apache.org/confluence/display/SAMOA/SAMOA+Home) 

