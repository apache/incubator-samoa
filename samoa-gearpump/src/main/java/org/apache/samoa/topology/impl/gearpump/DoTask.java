package org.apache.samoa.topology.impl.gearpump;

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

import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.streaming.StreamApplication;
import org.apache.gearpump.util.Graph;

public class DoTask {

  public static void main(String[] args) {

    GearpumpTopology topology = Utils.argsToTopology(args);
    String topologyName = topology.getTopologyName();
    Graph graph = topology.getGraph();
    StreamApplication app = StreamApplication.apply(topologyName, graph, UserConfig.empty());
    ClientContext context = ClientContext.apply();
    context.submit(app);
    context.close();

  }

}
