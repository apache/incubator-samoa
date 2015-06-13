package org.apache.samoa.moa.classifiers.core.driftdetection;

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

import org.apache.samoa.moa.options.AbstractOptionHandler;

/**
 * Abstract Change Detector. All change detectors in MOA extend this class.
 * 
 * @author Albert Bifet (abifet at cs dot waikato dot ac dot nz)
 * @version $Revision: 7 $
 */
public abstract class AbstractChangeDetector extends AbstractOptionHandler
    implements ChangeDetector {

  /**
   * Change was detected
   */
  protected boolean isChangeDetected;

  /**
   * Warning Zone: after a warning and before a change
   */
  protected boolean isWarningZone;

  /**
   * Prediction for the next value based in previous seen values
   */
  protected double estimation;

  /**
   * Delay in detecting change
   */
  protected double delay;

  /**
   * Resets this change detector. It must be similar to starting a new change detector from scratch.
   * 
   */
  public void resetLearning() {
    this.isChangeDetected = false;
    this.isWarningZone = false;
    this.estimation = 0.0;
    this.delay = 0.0;
  }

  /**
   * Adding a numeric value to the change detector<br>
   * <br>
   * 
   * The output of the change detector is modified after the insertion of a new item inside.
   * 
   * @param inputValue
   *          the number to insert into the change detector
   */
  public abstract void input(double inputValue);

  /**
   * Gets whether there is change detected.
   * 
   * @return true if there is change
   */
  public boolean getChange() {
    return this.isChangeDetected;
  }

  /**
   * Gets whether the change detector is in the warning zone, after a warning alert and before a change alert.
   * 
   * @return true if the change detector is in the warning zone
   */
  public boolean getWarningZone() {
    return this.isWarningZone;
  }

  /**
   * Gets the prediction of next values.
   * 
   * @return a prediction of the next value
   */
  public double getEstimation() {
    return this.estimation;
  }

  /**
   * Gets the length of the delay in the change detected.
   * 
   * @return he length of the delay in the change detected
   */
  public double getDelay() {
    return this.delay;
  }

  /**
   * Gets the output state of the change detection.
   * 
   * @return an array with the number of change detections, number of warnings, delay, and estimation.
   */
  public double[] getOutput() {
    return new double[] { this.isChangeDetected ? 1 : 0, this.isWarningZone ? 1 : 0, this.delay, this.estimation };
  }

  /**
   * Returns a string representation of the model.
   * 
   * @param sb
   *          the stringbuilder to add the description
   * @param indent
   *          the number of characters to indent
   */
  @Override
  public abstract void getDescription(StringBuilder sb, int indent);

  /**
   * Produces a copy of this change detector method
   * 
   * @return the copy of this change detector method
   */
  @Override
  public ChangeDetector copy() {
    return (ChangeDetector) super.copy();
  }
}
