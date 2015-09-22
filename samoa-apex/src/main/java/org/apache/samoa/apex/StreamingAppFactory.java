package org.apache.samoa.apex;

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

import com.datatorrent.api.StreamingApplication;

import com.datatorrent.stram.client.StramAppLauncher;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.logical.LogicalPlanConfiguration;

public class StreamingAppFactory implements StramAppLauncher.AppFactory {
 
  private StreamingApplication app;
  private String name;

  public StreamingAppFactory(StreamingApplication app, String name) {
    this.app = app;
    this.name = name;
  }

  public LogicalPlan createApp(LogicalPlanConfiguration planConfig) {
    LogicalPlan dag = new LogicalPlan();
    planConfig.prepareDAG(dag, app, getName());
    return dag;
  }

  public String getName() {
    return name;
  }

  public String getDisplayName() {
    return name;
  }
}
