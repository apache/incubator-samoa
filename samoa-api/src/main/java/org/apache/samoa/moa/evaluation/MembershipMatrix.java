package org.apache.samoa.moa.evaluation;

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
import java.util.HashMap;

import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.moa.core.DataPoint;

public class MembershipMatrix {

  HashMap<Integer, Integer> classmap;
  int cluster_class_weights[][];
  int cluster_sums[];
  int class_sums[];
  int total_entries;
  int class_distribution[];
  int total_class_entries;
  int initialBuildTimestamp = -1;

  public MembershipMatrix(Clustering foundClustering, ArrayList<DataPoint> points) {
    classmap = Clustering.classValues(points);
    // int lastID = classmap.size()-1;
    // classmap.put(-1, lastID);
    int numClasses = classmap.size();
    int numCluster = foundClustering.size() + 1;

    cluster_class_weights = new int[numCluster][numClasses];
    class_distribution = new int[numClasses];
    cluster_sums = new int[numCluster];
    class_sums = new int[numClasses];
    total_entries = 0;
    total_class_entries = points.size();
    for (DataPoint point : points) {
      int worklabel = classmap.get((int) point.classValue());
      // real class distribution
      class_distribution[worklabel]++;
      boolean covered = false;
      for (int c = 0; c < numCluster - 1; c++) {
        double prob = foundClustering.get(c).getInclusionProbability(point);
        if (prob >= 1) {
          cluster_class_weights[c][worklabel]++;
          class_sums[worklabel]++;
          cluster_sums[c]++;
          total_entries++;
          covered = true;
        }
      }
      if (!covered) {
        cluster_class_weights[numCluster - 1][worklabel]++;
        class_sums[worklabel]++;
        cluster_sums[numCluster - 1]++;
        total_entries++;
      }

    }

    initialBuildTimestamp = points.get(0).getTimestamp();
  }

  public int getClusterClassWeight(int i, int j) {
    return cluster_class_weights[i][j];
  }

  public int getClusterSum(int i) {
    return cluster_sums[i];
  }

  public int getClassSum(int j) {
    return class_sums[j];
  }

  public int getClassDistribution(int j) {
    return class_distribution[j];
  }

  public int getClusterClassWeightByLabel(int cluster, int classLabel) {
    return cluster_class_weights[cluster][classmap.get(classLabel)];
  }

  public int getClassSumByLabel(int classLabel) {
    return class_sums[classmap.get(classLabel)];
  }

  public int getClassDistributionByLabel(int classLabel) {
    return class_distribution[classmap.get(classLabel)];
  }

  public int getTotalEntries() {
    return total_entries;
  }

  public int getNumClasses() {
    return classmap.size();
  }

  public boolean hasNoiseClass() {
    return classmap.containsKey(-1);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Membership Matrix\n");
    for (int i = 0; i < cluster_class_weights.length; i++) {
      for (int j = 0; j < cluster_class_weights[i].length; j++) {
        sb.append(cluster_class_weights[i][j] + "\t ");
      }
      sb.append("| " + cluster_sums[i] + "\n");
    }
    // sb.append("-----------\n");
    for (int class_sum : class_sums) {
      sb.append(class_sum + "\t ");
    }
    sb.append("| " + total_entries + "\n");

    sb.append("Real class distribution \n");
    for (int aClass_distribution : class_distribution) {
      sb.append(aClass_distribution + "\t ");
    }
    sb.append("| " + total_class_entries + "\n");

    return sb.toString();
  }

  public int getInitialBuildTimestamp() {
    return initialBuildTimestamp;
  }

}
