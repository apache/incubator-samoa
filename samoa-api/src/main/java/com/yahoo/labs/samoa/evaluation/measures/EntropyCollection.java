package com.yahoo.labs.samoa.evaluation.measures;

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

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.labs.samoa.moa.cluster.Clustering;
import com.yahoo.labs.samoa.moa.core.DataPoint;
import com.yahoo.labs.samoa.moa.evaluation.MeasureCollection;
import com.yahoo.labs.samoa.moa.evaluation.MembershipMatrix;

public class EntropyCollection extends MeasureCollection {

  private static final Logger logger = LoggerFactory.getLogger(EntropyCollection.class);

  @Override
  protected String[] getNames() {
    return new String[] { "GT cross entropy", "FC cross entropy", "Homogeneity", "Completeness", "V-Measure",
        "VarInformation" };
  }

  @Override
  protected boolean[] getDefaultEnabled() {
    return new boolean[] { false, false, false, false, false, false };
  }

  @Override
  public void evaluateClustering(Clustering fclustering, Clustering hClustering, ArrayList<DataPoint> points)
      throws Exception {

    MembershipMatrix mm = new MembershipMatrix(fclustering, points);
    int numClasses = mm.getNumClasses();
    int numCluster = fclustering.size() + 1;
    int n = mm.getTotalEntries();

    double FCentropy = 0;
    if (numCluster > 1) {
      for (int fc = 0; fc < numCluster; fc++) {
        double weight = mm.getClusterSum(fc) / (double) n;
        if (weight > 0)
          FCentropy += weight * Math.log10(weight);
      }
      FCentropy /= (-1 * Math.log10(numCluster));
    }

    logger.debug("FC entropy: {}", FCentropy);

    double GTentropy = 0;
    if (numClasses > 1) {
      for (int hc = 0; hc < numClasses; hc++) {
        double weight = mm.getClassSum(hc) / (double) n;
        if (weight > 0)
          GTentropy += weight * Math.log10(weight);
      }
      GTentropy /= (-1 * Math.log10(numClasses));
    }

    logger.debug("GT entropy: {}", GTentropy);

    // cluster based entropy
    double FCcrossEntropy = 0;

    for (int fc = 0; fc < numCluster; fc++) {
      double e = 0;
      int clusterWeight = mm.getClusterSum(fc);
      if (clusterWeight > 0) {
        for (int hc = 0; hc < numClasses; hc++) {
          double p = mm.getClusterClassWeight(fc, hc) / (double) clusterWeight;
          if (p != 0) {
            e += p * Math.log10(p);
          }
        }
        FCcrossEntropy += ((clusterWeight / (double) n) * e);
      }
    }
    if (numCluster > 1) {
      FCcrossEntropy /= -1 * Math.log10(numCluster);
    }

    addValue("FC cross entropy", 1 - FCcrossEntropy);
    logger.debug("FC cross entropy: {}", 1 - FCcrossEntropy);

    // class based entropy
    double GTcrossEntropy = 0;
    for (int hc = 0; hc < numClasses; hc++) {
      double e = 0;
      int classWeight = mm.getClassSum(hc);
      if (classWeight > 0) {
        for (int fc = 0; fc < numCluster; fc++) {
          double p = mm.getClusterClassWeight(fc, hc) / (double) classWeight;
          if (p != 0) {
            e += p * Math.log10(p);
          }
        }
      }
      GTcrossEntropy += ((classWeight / (double) n) * e);
    }
    if (numClasses > 1)
      GTcrossEntropy /= -1 * Math.log10(numClasses);
    addValue("GT cross entropy", 1 - GTcrossEntropy);
    logger.debug("GT cross entropy: {}", 1 - GTcrossEntropy);

    double homogeneity;
    if (FCentropy == 0)
      homogeneity = 1;
    else
      homogeneity = 1 - FCcrossEntropy / FCentropy;

    // TODO set err values for now, needs to be debugged
    if (homogeneity > 1 || homogeneity < 0)
      addValue("Homogeneity", -1);
    else
      addValue("Homogeneity", homogeneity);

    double completeness;
    if (GTentropy == 0)
      completeness = 1;
    else
      completeness = 1 - GTcrossEntropy / GTentropy;
    addValue("Completeness", completeness);

    double beta = 1;
    double vmeasure = (1 + beta) * homogeneity * completeness / (beta * homogeneity + completeness);

    if (vmeasure > 1 || homogeneity < 0)
      addValue("V-Measure", -1);
    else
      addValue("V-Measure", vmeasure);

    double mutual = 0;
    for (int i = 0; i < numCluster; i++) {
      for (int j = 0; j < numClasses; j++) {
        if (mm.getClusterClassWeight(i, j) == 0)
          continue;
        double m = Math.log10(mm.getClusterClassWeight(i, j) / (double) mm.getClusterSum(i)
            / (double) mm.getClassSum(j) * (double) n);
        m *= mm.getClusterClassWeight(i, j) / (double) n;
        logger.debug("( {} / {}): ", m, m);
        mutual += m;
      }
    }
    if (numClasses > 1)
      mutual /= Math.log10(numClasses);

    double varInfo = 1;
    if (FCentropy + GTentropy > 0)
      varInfo = 2 * mutual / (FCentropy + GTentropy);

    logger.debug("mutual: {} / VI: {}", mutual, varInfo);
    addValue("VarInformation", varInfo);

  }

}
