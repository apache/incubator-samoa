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

package org.apache.samoa.evaluation.measures;

import java.util.ArrayList;

import org.apache.samoa.moa.cluster.Cluster;
import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.moa.core.DataPoint;
import org.apache.samoa.moa.evaluation.MeasureCollection;

public class SilhouetteCoefficient extends MeasureCollection {
  private static final double pointInclusionProbThreshold = 0.8;

  public SilhouetteCoefficient() {
    super();
  }

  @Override
  protected boolean[] getDefaultEnabled() {
    return new boolean[] { false };
  }

  @Override
  public String[] getNames() {
    return new String[] { "SilhCoeff" };
  }

  public void evaluateClustering(Clustering clustering, Clustering trueClustering, ArrayList<DataPoint> points) {
    int numFCluster = clustering.size();

    double[][] pointInclusionProbFC = new double[points.size()][numFCluster];
    for (int p = 0; p < points.size(); p++) {
      DataPoint point = points.get(p);
      for (int fc = 0; fc < numFCluster; fc++) {
        Cluster cl = clustering.get(fc);
        pointInclusionProbFC[p][fc] = cl.getInclusionProbability(point);
      }
    }

    double silhCoeff = 0.0;
    int totalCount = 0;
    for (int p = 0; p < points.size(); p++) {
      DataPoint point = points.get(p);
      ArrayList<Integer> ownClusters = new ArrayList<>();
      for (int fc = 0; fc < numFCluster; fc++) {
        if (pointInclusionProbFC[p][fc] > pointInclusionProbThreshold) {
          ownClusters.add(fc);
        }
      }

      if (ownClusters.size() > 0) {
        double[] distanceByClusters = new double[numFCluster];
        int[] countsByClusters = new int[numFCluster];
        // calculate averageDistance of p to all cluster
        for (int p1 = 0; p1 < points.size(); p1++) {
          DataPoint point1 = points.get(p1);
          if (p1 != p && point1.classValue() != -1) {
            for (int fc = 0; fc < numFCluster; fc++) {
              if (pointInclusionProbFC[p1][fc] > pointInclusionProbThreshold) {
                double distance = point.getDistance(point1);
                distanceByClusters[fc] += distance;
                countsByClusters[fc]++;
              }
            }
          }
        }

        // find closest OWN cluster as clusters might overlap
        double minAvgDistanceOwn = Double.MAX_VALUE;
        int minOwnIndex = -1;
        for (int fc : ownClusters) {
          double normDist = distanceByClusters[fc] / (double) countsByClusters[fc];
          if (normDist < minAvgDistanceOwn) {// && pointInclusionProbFC[p][fc] > pointInclusionProbThreshold){
            minAvgDistanceOwn = normDist;
            minOwnIndex = fc;
          }
        }

        // find closest other (or other own) cluster
        double minAvgDistanceOther = Double.MAX_VALUE;
        for (int fc = 0; fc < numFCluster; fc++) {
          if (fc != minOwnIndex) {
            double normDist = distanceByClusters[fc] / (double) countsByClusters[fc];
            if (normDist < minAvgDistanceOther) {
              minAvgDistanceOther = normDist;
            }
          }
        }

        double silhP = (minAvgDistanceOther - minAvgDistanceOwn) / Math.max(minAvgDistanceOther, minAvgDistanceOwn);
        point.setMeasureValue("SC - own", minAvgDistanceOwn);
        point.setMeasureValue("SC - other", minAvgDistanceOther);
        point.setMeasureValue("SC", silhP);

        silhCoeff += silhP;
        totalCount++;
        // System.out.println(point.getTimestamp()+" Silh "+silhP+" / "+avgDistanceOwn+" "+minAvgDistanceOther+" (C"+minIndex+")");
      }
    }
    if (totalCount > 0)
      silhCoeff /= (double) totalCount;
    // normalize from -1, 1 to 0,1
    silhCoeff = (silhCoeff + 1) / 2.0;
    addValue(0, silhCoeff);
  }

}
