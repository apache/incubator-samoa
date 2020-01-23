/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samoa.evaluation;

import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.core.Utils;
import org.apache.samoa.moa.core.Vote;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.moa.tasks.TaskMonitor;

public class TimeClassificationPerformanceEvaluator extends AbstractOptionHandler
        implements ClassificationPerformanceEvaluator {
    private int numClasses;
    private PredictionAggregator aggregator;
    private Indicator[] indicators;
    
    public FloatOption maxProcessingTimeOption = new FloatOption("maxProcessingTime",
            'l', "Processing time limit per instance [ms]", 3600e3, 0, 3600e3);
    
    public FloatOption timeWeightOption = new FloatOption("timeWeight",
            'a', "How many times speed is more important than accuracy", 1, 0, 100);
    
    public IntOption widthOption = new IntOption("width", 'w', 
            "Size of Window", 1000);
    
    @Override
    public void reset() {
        double timeLimit = maxProcessingTimeOption.getValue();
        double weight = timeWeightOption.getValue();
        
        aggregator = new PredictionAggregator(numClasses, new EstimatorFactory() {
            @Override
            public WindowEstimator create() {
                int w = widthOption.getValue();
                return new WindowEstimator(w);
            }
        });
        aggregator.setProcessingTimeLimit(timeLimit);
        Indicator timeIndicator = new TimeIndicator(aggregator, timeLimit);
        KappaIndicator kappaIndicator = new KappaIndicator(aggregator);
        indicators = new Indicator[] {
            new AccuracyIndicator(aggregator),  
            kappaIndicator,
            new KappaMIndicator(aggregator),
            timeIndicator,
            new TooLatePredictionsIndicator(aggregator),
            new WeightedSumIndicator(new Indicator[]{
                kappaIndicator,
                new KappaMIndicator(aggregator),
                timeIndicator
            }, new double[] {0.5, 0.5, -weight}),
            new EfficiencyIndicator(new WeightedSumIndicator(
                    new Indicator[]{
                        kappaIndicator,
                        new KappaMIndicator(aggregator)
                    },
                    new double[]{0.5, 0.5}
            ), new WeightedSumIndicator(
                    new Indicator[]{ timeIndicator },
                    new double[]{weight}
            )),
            new SimpleIntuitiveIndicator(new Indicator[]{
                new OneMinusKappaIndicator(kappaIndicator),
                timeIndicator
            }, new double[]{1, 0}, new double[]{0, 0}, new double[]{1, 1})
        };
    }

    @Override
    public Measurement[] getPerformanceMeasurements() {
        Measurement[] meas = new Measurement[indicators.length];
        for(int i=0;i<indicators.length;++i)
            meas[i] = new Measurement(indicators[i].getDescription(), indicators[i].getValue());
        return meas;
    }
    
    @Override
    public void getDescription(StringBuilder sb, int indent) {
        Measurement.getMeasurementsDescription(getPerformanceMeasurements(),
                sb, indent);
    }

    @Override
    protected void prepareForUseImpl(TaskMonitor monitor, ObjectRepository repository) {
        
    }

    @Override
    public void addResult(Instance inst, double[] classVotes, String instanceIdentifier, long delay) {
        if(indicators == null) {
            numClasses = inst.numClasses();
            reset();
        }
        aggregator.add(inst, classVotes, delay);
    }

    @Override
    public Vote[] getPredictionVotes() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}

interface EstimatorFactory {
    WindowEstimator create();
}

class PredictionAggregator {
    private final int numClasses;
    private final WindowEstimator weightCorrect;
    private final WindowEstimator[] columnKappa;
    private final WindowEstimator[] rowKappa;
    private final WindowEstimator weightMajorityClassifier;
    private final WindowEstimator weightNoChangeClassifier;
    private final WindowEstimator processingTime;
    private final WindowEstimator delayedFraction;
    private int lastSeenClass;
    private int numClassifiedInstances;
    private double processingTimeLimit;
    
    public PredictionAggregator(int numClasses, EstimatorFactory factory) {
        this.numClasses = numClasses;
        columnKappa = new WindowEstimator[numClasses];
        rowKappa = new WindowEstimator[numClasses];
        for (int i = 0; i < numClasses; i++) {
            this.rowKappa[i] = factory.create();
            this.columnKappa[i] = factory.create();
        }
        weightCorrect = factory.create();
        weightMajorityClassifier = factory.create();
        weightNoChangeClassifier = factory.create();
        processingTime = factory.create();
        processingTimeLimit = Double.MAX_VALUE;
        delayedFraction = factory.create();
        numClassifiedInstances = 0;
    }
    
    public void add(Instance inst, double[] votes, long delay) {
        if(inst.classIsMissing())
            return;
        double weight = inst.weight();
        int trueClass = (int) inst.classValue();
        int predictedClass = Utils.maxIndex(votes);
        if(delay > processingTimeLimit) {
            predictedClass = -1;
            delayedFraction.add(weight);
        } else {
            delayedFraction.add(0);
        }
        for (int i = 0; i < numClasses; i++) {
            this.rowKappa[i].add(predictedClass == i ? weight : 0);
            this.columnKappa[i].add(trueClass == i ? weight : 0);
        }
        weightMajorityClassifier.add(getMajorityClass() == trueClass ? weight : 0);
        weightCorrect.add(trueClass == predictedClass ? weight : 0);
        weightNoChangeClassifier.add(trueClass == lastSeenClass ? weight : 0);
        processingTime.add(delay);
        lastSeenClass = trueClass;
        ++numClassifiedInstances;
    }
    
    public int getNumClasses() {
        return numClasses;
    }
    
    public int getNumClassifiedInstances() {
        return numClassifiedInstances;
    }
    
    public double getMeanCorrect() {
        return weightCorrect.estimation();
    }
    
    public double getMeanMajorityCorrect() {
        return weightMajorityClassifier.estimation();
    }
    
    public double getMeanRow(int rowNumber) {
        return rowKappa[rowNumber].estimation();
    }
    
    public double getMeanColumn(int columnNumber) {
        return columnKappa[columnNumber].estimation();
    }
    
    public double getMeanProcessingTime() {
        return processingTime.estimation();
    }
    
    public void setProcessingTimeLimit(double limit) {
        this.processingTimeLimit = limit;
    }
    
    public double getDelayedFraction() {
        return delayedFraction.estimation();
    }
    
    private int getMajorityClass() {
        int majorityClass = 0;
        double maxProbClass = 0.0;
        for (int i = 0; i < this.numClasses; i++) {
            if (this.columnKappa[i].estimation() > maxProbClass) {
                majorityClass = i;
                maxProbClass = this.columnKappa[i].estimation();
            }
        }
        return majorityClass;
    }
}

interface Indicator {
    String getDescription();
    double getValue();
}

class AccuracyIndicator implements Indicator {
    private final PredictionAggregator aggregator;
    
    public AccuracyIndicator(PredictionAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public String getDescription() {
        return "Accuracy";
    }

    @Override
    public double getValue() {
        return aggregator.getMeanCorrect();
    }
}

class KappaIndicator implements Indicator {
    private final PredictionAggregator aggregator;
    
    public KappaIndicator(PredictionAggregator aggregator) {
        this.aggregator = aggregator;   
    }
    
    @Override
    public String getDescription() {
        return "Kappa Statistic";
    }
    
    @Override
    public double getValue() {
        double p0 = aggregator.getMeanCorrect();
        double pc = 0;
        for(int i=0;i<aggregator.getNumClasses();++i)
            pc += aggregator.getMeanRow(i)*aggregator.getMeanColumn(i);
        return (p0-pc)/(1-pc);
    }
}

class OneMinusKappaIndicator implements Indicator {
    private final KappaIndicator indicator;
    
    public OneMinusKappaIndicator(KappaIndicator indicator) {
        this.indicator = indicator;
    }

    @Override
    public String getDescription() {
        return "1-Kappa Statistic";
    }

    @Override
    public double getValue() {
        return 1-indicator.getValue();
    }
}

class KappaMIndicator implements Indicator {
    private final PredictionAggregator aggregator;
    
    public KappaMIndicator(PredictionAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public String getDescription() {
        return "Kappa M Statistic";
    }

    @Override
    public double getValue() {
        double p0 = aggregator.getMeanCorrect();
        double pc = aggregator.getMeanMajorityCorrect();
        return (p0-pc)/(1-pc);
    }    
}

class TimeIndicator implements Indicator {
    private final PredictionAggregator aggregator;
    private final double timeUnit;
    
    public TimeIndicator(PredictionAggregator aggregator, double timeUnit) {
        this.aggregator = aggregator;
        this.timeUnit = timeUnit;
    }

    @Override
    public String getDescription() {
        return "Mean processing time";
    }

    @Override
    public double getValue() {
        return aggregator.getMeanProcessingTime() / timeUnit;
    }
}

class TooLatePredictionsIndicator implements Indicator {
    private final PredictionAggregator aggregator;
    
    public TooLatePredictionsIndicator(PredictionAggregator aggregator) {
        this.aggregator = aggregator;
    }

    @Override
    public String getDescription() {
        return "Too late predictions";
    }

    @Override
    public double getValue() {
        return aggregator.getDelayedFraction();
    }
}


///////////// aggregators
class WeightedSumIndicator implements Indicator {
    private Indicator[] indicators;
    private double[] weights;
    
    public WeightedSumIndicator(Indicator[] indicators, double[] weights) {
        this.indicators = indicators;
        this.weights = weights;
    }

    @Override
    public String getDescription() {
        return "Measure-Based";
    }

    @Override
    public double getValue() {
        double sum = 0;
        for(int i=0;i < indicators.length;++i)
            sum += weights[i] * indicators[i].getValue();
        return sum;
    }
    
    
}

class EfficiencyIndicator implements Indicator {
    private WeightedSumIndicator positive;
    private WeightedSumIndicator negative;
    
    public EfficiencyIndicator(WeightedSumIndicator positive, WeightedSumIndicator negative) {
        this.positive = positive;
        this.negative = negative;
    }

    @Override
    public String getDescription() {
        return "Efficiency Indicator";
    }

    @Override
    public double getValue() {
        return positive.getValue() / negative.getValue();
    }
}

class SimpleIntuitiveIndicator implements Indicator {
    private Indicator[] indicators;
    private double[] optimums;
    private double[] lBounds;
    private double[] uBounds;
    
    public SimpleIntuitiveIndicator(Indicator[] indicators, double[] optimums) {
        this.indicators = indicators;
        this.optimums = optimums;
        lBounds = new double[optimums.length];
        uBounds = new double[optimums.length];
        for(int i=0;i<lBounds.length;++i) {
            lBounds[i] = Double.MIN_VALUE;
            uBounds[i] = Double.MAX_VALUE;
        }
    }
    
    public SimpleIntuitiveIndicator(Indicator[] indicators, double[] optimums,
            double[] lBounds, double[] uBounds) {
        this(indicators, optimums);
        this.lBounds = lBounds;
        this.uBounds = uBounds;
    }
    
    public void adjustBound(int i, double lower, double upper) {
        lBounds[i] = lower;
        uBounds[i] = upper;
    }
    
    @Override
    public String getDescription() {
        return "Simple Intuitive Measure";
    }

    @Override
    public double getValue() {
        double prod = 1, worst = 1;
        for(int i=0;i<indicators.length;++i) {
            double val = indicators[i].getValue();
            if(val < lBounds[i] || val > uBounds[i])
                return Double.MAX_VALUE;
            prod *= Math.abs(indicators[i].getValue() - optimums[i]);
            if(lBounds[i] != Double.MIN_VALUE && uBounds[i] != Double.MAX_VALUE)
                worst *= uBounds[i] - lBounds[i];
        }
        return prod / worst;
    }
    
}
