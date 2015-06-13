package org.apache.samoa.moa.cluster;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.samoa.instances.Attribute;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.moa.AbstractMOAObject;
import org.apache.samoa.moa.core.AutoExpandVector;
import org.apache.samoa.moa.core.DataPoint;

public class Clustering extends AbstractMOAObject {

  private AutoExpandVector<Cluster> clusters;

  public Clustering() {
    this.clusters = new AutoExpandVector<>();
  }

  public Clustering(Cluster[] clusters) {
    this.clusters = new AutoExpandVector<>();
    Collections.addAll(this.clusters, clusters);
  }

  public Clustering(List<? extends Instance> points) {
    HashMap<Integer, Integer> labelMap = classValues(points);
    int dim = points.get(0).dataset().numAttributes() - 1;

    int numClasses = labelMap.size();
    int noiseLabel;

    Attribute classLabel = points.get(0).dataset().classAttribute();
    int lastLabelIndex = classLabel.numValues() - 1;
    if ("noise".equalsIgnoreCase(classLabel.value(lastLabelIndex))) {
      noiseLabel = lastLabelIndex;
    } else {
      noiseLabel = -1;
    }

    ArrayList<Instance>[] sorted_points = (ArrayList<Instance>[]) new ArrayList[numClasses];
    for (int i = 0; i < numClasses; i++) {
      sorted_points[i] = new ArrayList<>();
    }

    for (Instance point : points) {
      int clusterId = (int) point.classValue();
      if (clusterId != noiseLabel) {
        sorted_points[labelMap.get(clusterId)].add(point);
      }
    }

    this.clusters = new AutoExpandVector<>();
    for (int i = 0; i < numClasses; i++) {
      if (sorted_points[i].size() > 0) {
        SphereCluster s = new SphereCluster(sorted_points[i], dim);
        s.setId(sorted_points[i].get(0).classValue());
        s.setGroundTruth(sorted_points[i].get(0).classValue());
        clusters.add(s);
      }
    }
  }

  public Clustering(ArrayList<DataPoint> points, double overlapThreshold, int initMinPoints) {
    HashMap<Integer, Integer> labelMap = Clustering.classValues(points);
    int dim = points.get(0).dataset().numAttributes() - 1;

    int numClasses = labelMap.size();

    ArrayList<DataPoint>[] sorted_points = (ArrayList<DataPoint>[]) new ArrayList[numClasses];
    for (int i = 0; i < numClasses; i++) {
      sorted_points[i] = new ArrayList<>();
    }

    for (DataPoint point : points) {
      int clusterId = (int) point.classValue();
      if (clusterId != -1) {
        sorted_points[labelMap.get(clusterId)].add(point);
      }
    }

    clusters = new AutoExpandVector<>();
    for (int i = 0; i < numClasses; i++) {
      ArrayList<SphereCluster> microByClass = new ArrayList<>();
      ArrayList<DataPoint> pointInCluster = new ArrayList<>();
      ArrayList<ArrayList<Instance>> pointInMicroClusters = new ArrayList<>();

      pointInCluster.addAll(sorted_points[i]);
      while (pointInCluster.size() > 0) {
        ArrayList<Instance> micro_points = new ArrayList<>();
        for (int j = 0; j < initMinPoints && !pointInCluster.isEmpty(); j++) {
          micro_points.add(pointInCluster.get(0));
          pointInCluster.remove(0);
        }
        if (micro_points.size() > 0) {
          SphereCluster s = new SphereCluster(micro_points, dim);
          for (int c = 0; c < microByClass.size(); c++) {
            if ((microByClass.get(c)).overlapRadiusDegree(s) > overlapThreshold) {
              micro_points.addAll(pointInMicroClusters.get(c));
              s = new SphereCluster(micro_points, dim);
              pointInMicroClusters.remove(c);
              microByClass.remove(c);
            }
          }

          for (int j = 0; j < pointInCluster.size(); j++) {
            Instance instance = pointInCluster.get(j);
            if (s.getInclusionProbability(instance) > 0.8) {
              pointInCluster.remove(j);
              micro_points.add(instance);
            }
          }
          s.setWeight(micro_points.size());
          microByClass.add(s);
          pointInMicroClusters.add(micro_points);
        }
      }
      //
      boolean changed = true;
      while (changed) {
        changed = false;
        for (int c = 0; c < microByClass.size(); c++) {
          for (int c1 = c + 1; c1 < microByClass.size(); c1++) {
            double overlap = microByClass.get(c).overlapRadiusDegree(microByClass.get(c1));
            if (overlap > overlapThreshold) {
              pointInMicroClusters.get(c).addAll(pointInMicroClusters.get(c1));
              SphereCluster s = new SphereCluster(pointInMicroClusters.get(c), dim);
              microByClass.set(c, s);
              pointInMicroClusters.remove(c1);
              microByClass.remove(c1);
              changed = true;
              break;
            }
          }
        }
      }

      for (SphereCluster microByClas : microByClass) {
        microByClas.setGroundTruth(sorted_points[i].get(0).classValue());
        clusters.add(microByClas);
      }
    }

    for (int j = 0; j < clusters.size(); j++) {
      clusters.get(j).setId(j);
    }

  }

  /**
   * @param points - points to be clustered
   * @return an array with the min and max class label value
   */
  public static HashMap<Integer, Integer> classValues(List<? extends Instance> points) {
    HashMap<Integer, Integer> classes = new HashMap<>();
    int workCluster = 0;
    boolean hasNoise = false;
    for (Instance point : points) {
      int label = (int) point.classValue();
      if (label == -1) {
        hasNoise = true;
      } else {
        if (!classes.containsKey(label)) {
          classes.put(label, workCluster);
          workCluster++;
        }
      }
    }

    if (hasNoise) {
      classes.put(-1, workCluster);
    }
    return classes;
  }

  public Clustering(AutoExpandVector<Cluster> clusters) {
    this.clusters = clusters;
  }

  /**
   * add a cluster to the clustering
   */
  public void add(Cluster cluster) {
    clusters.add(cluster);
  }

  /**
   * remove a cluster from the clustering
   */
  public void remove(int index) {
    if (index < clusters.size()) {
      clusters.remove(index);
    }
  }

  /**
   * remove a cluster from the clustering
   */
  public Cluster get(int index) {
    if (index < clusters.size()) {
      return clusters.get(index);
    }
    return null;
  }

  /**
   * @return the <code>Clustering</code> as an AutoExpandVector
   */
  public AutoExpandVector<Cluster> getClustering() {
    return clusters;
  }

  /**
   * @return A deepcopy of the <code>Clustering</code> as an AutoExpandVector
   */
  public AutoExpandVector<Cluster> getClusteringCopy() {
    return (AutoExpandVector<Cluster>) clusters.copy();
  }

  /**
   * @return the number of clusters
   */
  public int size() {
    return clusters.size();
  }

  /**
   * @return the number of dimensions of this clustering
   */
  public int dimension() {
    assert (clusters.size() != 0);
    return clusters.get(0).getCenter().length;
  }

  @Override
  public void getDescription(StringBuilder sb, int i) {
    sb.append("Clustering Object");
  }

  public double getMaxInclusionProbability(Instance point) {
    double maxInclusion = 0.0;
    for (Cluster cluster : clusters) {
      maxInclusion = Math.max(cluster.getInclusionProbability(point), maxInclusion);
    }
    return maxInclusion;
  }

}
