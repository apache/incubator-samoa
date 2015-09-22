package org.apache.samoa.apex;

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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.samoa.apex.topology.impl.ApexTask;
import org.apache.samoa.apex.topology.impl.ApexTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.LocalMode;
import com.datatorrent.stram.plan.logical.LogicalPlan;

public class LocalApexDoTask {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(LocalApexDoTask.class);

  public static void main(String[] args) {

    List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));

    args = tmpArgs.toArray(new String[0]);

    ApexTopology apexTopo = ApexSamoaUtils.argsToTopology(args);

    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
//    conf.set("dt.loggers.level", "org.apache.*:DEBUG");

    try {
      lma.prepareDAG(new ApexTask(apexTopo), conf);
      System.out.println("Dag Set in lma: " + lma.getDAG());
      ((LogicalPlan) lma.getDAG()).validate();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    lc.runAsync();
  }
}
