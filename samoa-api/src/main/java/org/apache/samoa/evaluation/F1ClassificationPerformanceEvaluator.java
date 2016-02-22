package org.apache.samoa.evaluation;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2014 - 2016 Apache Software Foundation
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


import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Utils;
import org.apache.samoa.moa.AbstractMOAObject;
import org.apache.samoa.moa.core.Measurement;

/**
 * Created by Edi Bice (edi.bice gmail com) on 2/22/2016.
 */
public class F1ClassificationPerformanceEvaluator extends AbstractMOAObject implements
        ClassificationPerformanceEvaluator {

    private static final long serialVersionUID = 1L;
    protected int numClasses;

    protected long[] support;
    protected long[] truePos;
    protected long[] falsePos;
    protected long[] trueNeg;
    protected long[] falseNeg;

    @Override
    public void reset() {
        reset(this.numClasses);
    }

    public void reset(int numClasses) {
        this.numClasses = numClasses;
        this.truePos = new long[numClasses];
        this.falsePos = new long[numClasses];
        this.trueNeg = new long[numClasses];
        this.falseNeg = new long[numClasses];
        for (int i = 0; i < this.numClasses; i++) {
            this.support[i] = 0;
            this.truePos[i] = 0;
            this.falsePos[i] = 0;
            this.trueNeg[i] = 0;
            this.falseNeg[i] = 0;
        }
    }

    @Override
    public void addResult(Instance inst, double[] classVotes) {
        int trueClass = (int) inst.classValue();
        this.support[trueClass] += 1;
        int predictedClass = Utils.maxIndex(classVotes);
        if (predictedClass == trueClass) {
            this.truePos[trueClass] += 1;
            for (int i = 0; i < this.numClasses; i++) {
                if (i!=predictedClass) this.trueNeg[i] += 1;
            }
        } else {
            this.falsePos[predictedClass] += 1;
            this.falseNeg[trueClass] += 1;
            for (int i = 0; i < this.numClasses; i++) {
                if (!(i==predictedClass || i==trueClass)) this.trueNeg[i] += 1;
            }
        }
    }

    @Override
    public Measurement[] getPerformanceMeasurements() {
        return getF1Measurements();
    }

    private Measurement[] getSupportMeasurements() {
        Measurement[] measurements = new Measurement[this.numClasses];
        for (int i = 0; i < this.numClasses; i++) {
            String ml = String.format("class %s support", i);
            measurements[i] = new Measurement(ml, this.support[i]);
        }
        return measurements;
    }

    private Measurement[] getPrecisionMeasurements() {
        Measurement[] measurements = new Measurement[this.numClasses];
        for (int i = 0; i < this.numClasses; i++) {
            String ml = String.format("class %s precision", i);
            measurements[i] = new Measurement(ml, getPrecision(i));
        }
        return measurements;
    }

    private Measurement[] getRecallMeasurements() {
        Measurement[] measurements = new Measurement[this.numClasses];
        for (int i = 0; i < this.numClasses; i++) {
            String ml = String.format("class %s recall", i);
            measurements[i] = new Measurement(ml, getRecall(i));
        }
        return measurements;
    }

    private Measurement[] getF1Measurements() {
        Measurement[] measurements = new Measurement[this.numClasses];
        for (int i = 0; i < this.numClasses; i++) {
            String ml = String.format("class %s f1-score", i);
            measurements[i] = new Measurement(ml, getF1Score(i));
        }
        return measurements;
    }

    @Override
    public void getDescription(StringBuilder sb, int indent) {
        Measurement.getMeasurementsDescription(getSupportMeasurements(), sb, indent);
        Measurement.getMeasurementsDescription(getPrecisionMeasurements(), sb, indent);
        Measurement.getMeasurementsDescription(getRecallMeasurements(), sb, indent);
        Measurement.getMeasurementsDescription(getF1Measurements(), sb, indent);
    }

    private double getPrecision(int classIndex) {
        return (double) this.truePos[classIndex] / (this.truePos[classIndex] + this.falsePos[classIndex]);
    }

    private double getRecall(int classIndex) {
        return (double) this.truePos[classIndex] / (this.truePos[classIndex] + this.falseNeg[classIndex]);
    }

    private double getF1Score(int classIndex) {
        double precision = getPrecision(classIndex);
        double recall = getRecall(classIndex);
        return 2 * (precision * recall) / (precision + recall);
    }

}
