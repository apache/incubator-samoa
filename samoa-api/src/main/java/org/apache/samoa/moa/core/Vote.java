package org.apache.samoa.moa.core;

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
import java.util.List;

import org.apache.samoa.moa.AbstractMOAObject;

/**
 * Class for storing an evaluation measurement.
 * 
 * @author Richard Kirkby (rkirkby@cs.waikato.ac.nz)
 * @version $Revision: 7 $
 */
public class Vote extends AbstractMOAObject {

  private static final long serialVersionUID = 1L;

  protected String name;
  protected String value;
  //  protected int fractionDigits;

  public Vote(String name, String value) {
    this.name = name;
    this.value = value;
  }

  public Vote(String name, double value) {
    this(name, value, 3);
  }

  public Vote(String name, double value, int fractionDigits) {
    this(name, Double.toString(value));
  }

  public String getName() {
    return this.name;
  }

  public String getValue() {
    return this.value;
  }

  public static Vote getVoteNamed(String name,
      Vote[] votes) {
    for (Vote measurement : votes) {
      if (name.equals(measurement.getName())) {
        return measurement;
      }
    }
    return null;
  }

  public static void getVotesDescription(Vote[] votes,
      StringBuilder out, int indent) {
    if (votes.length > 0) {
      StringUtils.appendIndented(out, indent, votes[0].toString());
      for (int i = 1; i < votes.length; i++) {
        StringUtils.appendNewlineIndented(out, indent, votes[i].toString());
      }
    }
  }

  @Override
  public void getDescription(StringBuilder sb, int indent) {
    sb.append(getName());
    sb.append(" = ");
    sb.append(this.value);
  }
}
