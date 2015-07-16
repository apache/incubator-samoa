package org.apache.samoa.streams.kafka;

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

import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
import org.apache.samoa.instances.*;
import org.apache.samoa.moa.core.Example;
import org.apache.samoa.moa.core.FastVector;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.moa.streams.InstanceStream;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class KafkaStream extends AbstractOptionHandler implements
        InstanceStream {

    private static final long serialVersionUID = 1L;

    protected InstancesHeader streamHeader;

    protected Instances instances;

    private KafkaReader reader;

    protected InstanceExample lastInstanceRead;

    protected Queue<String> instanceQueue;

    private static final Logger logger = LoggerFactory
            .getLogger(KafkaStream.class);

    public void KafkaReader() {
        reader = new KafkaReader();
    }

    public IntOption classIndexOption = new IntOption("classIndex", 'c',
            "Class index of data. 0 for none or -1 for last attribute in file.",
            -1, -1, Integer.MAX_VALUE);

    public IntOption numAttrOption = new IntOption("numNumerics", 'u',
            "The number of numeric attributes in" +
                    " dataset", 300, 0,
            Integer.MAX_VALUE);

    public StringOption topicOption = new StringOption("topic", 't',
            "Topic in the kafka to be used for reading data", "test");

    public IntOption numMaxreadOption = new IntOption("numMaxread", 'r',
            "Number of instances to be read in single read from kafka", 1, 0,
            Integer.MAX_VALUE);

    public IntOption partitionOption = new IntOption("partition", 'n',
            "Partition number to be used for reading data", 0);

    public IntOption portOption = new IntOption("port", 'p',
            "Port in kafka to read data", 9092);

    public StringOption seedOption = new StringOption("seed", 's',
            "Seeds for kafka", "localhost");

    public IntOption numClassesOption = new IntOption("numClasses", 'k',
            "The number of classes in the data.", 2, 2, Integer.MAX_VALUE);

    public IntOption timeDelayOption = new IntOption("timeDelay", 'y',
            "Time delay in milliseconds between two read from kafka", 0, 0, Integer.MAX_VALUE);

    @Override
    protected void prepareForUseImpl(TaskMonitor monitor,
                                     ObjectRepository repository) {
        this.reader = new KafkaReader();
        generateHeader();
        instanceQueue = new LinkedList<String>();
        this.lastInstanceRead = null;
    }

    protected void generateHeader() {
        FastVector<Attribute> attributes = new FastVector<>();

        for (int i = 0; i < this.numAttrOption.getValue(); i++) {
            attributes.addElement(new Attribute("numeric" + (i + 1)));
        }
        FastVector<String> classLabels = new FastVector<>();
        for (int i = 0; i < this.numClassesOption.getValue(); i++) {
            classLabels.addElement("class" + (i + 1));
        }

        attributes.addElement(new Attribute("class", classLabels));
        this.streamHeader = new InstancesHeader(new Instances(
                getCLICreationString(InstanceStream.class), attributes, 0));

        if (this.classIndexOption.getValue() < 0) {
            this.streamHeader.setClassIndex(this.streamHeader.numAttributes() - 1);
        } else if (this.classIndexOption.getValue() > 0) {
            this.streamHeader.setClassIndex(this.classIndexOption.getValue() - 1);
        }

    }

    @Override
    public InstancesHeader getHeader() {
        return this.streamHeader;
    }

    @Override
    public long estimatedRemainingInstances() {
        return -1;
    }

    private String getNextInstanceFromKafka() {
        if (!instanceQueue.isEmpty()) {
            return instanceQueue.remove();
        }

        List<String> seeds = new ArrayList<String>();
        seeds.add(this.seedOption.getValue());
        ArrayList<String> kafkaData;

        do {
            kafkaData = this.reader.run(this.numMaxreadOption.getValue(),
                    this.topicOption.getValue(), this.partitionOption.getValue(),
                    seeds, this.portOption.getValue());
        } while (kafkaData == null);

        instanceQueue.addAll(kafkaData);
        return instanceQueue.remove();
    }

    @Override
    public Example<Instance> nextInstance() {
        InstancesHeader header = getHeader();
        Instance inst = new DenseInstance(header.numAttributes());

        String kafkaString = getNextInstanceFromKafka();
        String[] KeyValueString = kafkaString.split(":");
        String[] attributes = KeyValueString[1].split(",");

        for (int i = 0; i < attributes.length - 1; i++) {
            if (i < numAttrOption.getValue()) {
                inst.setValue(i, Double.parseDouble(attributes[i]));
            }
        }
        inst.setDataset(header);
        inst.setClassValue(Double
                .parseDouble(attributes[attributes.length - 1]));

        try {
            Thread.sleep(timeDelayOption.getValue());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return new InstanceExample(inst);
    }

    @Override
    public boolean isRestartable() {
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public void restart() {
        this.reader = new KafkaReader();
    }

    @Override
    public boolean hasMoreInstances() {
        return true;
    }

    @Override
    public void getDescription(StringBuilder sb, int indent) {
        // TODO Auto-generated method stub
    }
}
