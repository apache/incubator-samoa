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

public class WindowEstimator {
    protected double[] window;

    protected int posWindow;

    protected int lenWindow;

    protected int SizeWindow;

    protected double sum;

    public WindowEstimator(int sizeWindow) {
      window = new double[sizeWindow];
      SizeWindow = sizeWindow;
      posWindow = 0;
      lenWindow = 0;
    }

    public void add(double value) {
      sum -= window[posWindow];
      sum += value;
      window[posWindow] = value;
      posWindow++;
      if (posWindow == SizeWindow) {
        posWindow = 0;
      }
      if (lenWindow < SizeWindow) {
        lenWindow++;
      }
    }

    public double total() {
      return sum;
    }

    public double length() {
      return lenWindow;
    }
    
    public double estimation() {
        return sum / lenWindow;
    }
}
