package org.apache.samoa.heron.topology;
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

import org.apache.samoa.heron.topology.impl.HeronSamoaUtils;
import org.apache.samoa.heron.topology.impl.HeronTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.NotAliveException;


/**
 * The main class to execute a SAMOA task in LOCAL mode in Heron.
 *
 * @author Arinto Murdopo
 */
public class LocalHeronDoTask {
    private static final Logger logger = LoggerFactory.getLogger(LocalHeronDoTask.class);
    private static final String EXECUTION_DURATION_KEY = "samoa.storm.local.mode.execution.duration";
    private static final String SAMOA_STORM_PROPERTY_FILE_LOC = "samoa-heron.properties";

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) {
        List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));
        int numWorker = HeronSamoaUtils.numWorkers(tmpArgs);
        args = tmpArgs.toArray(new String[0]);
        // convert the arguments into Storm topology
        HeronTopology heronTopo = HeronSamoaUtils.argsToTopology(args);
        String topologyName = heronTopo.getTopologyName();
        Config conf = new Config();
        // conf.putAll(Utils.readStormConfig());
        conf.setDebug(false);
        // local mode
        conf.setMaxTaskParallelism(numWorker);
        LocalCluster cluster = new LocalCluster();
        try {
            cluster.submitTopology(topologyName, conf, heronTopo.getHeronBuilder().createTopology());
            // Read local mode execution duration from property file
            Configuration heronConfig = HeronSamoaUtils.getPropertyConfig(LocalHeronDoTask.SAMOA_STORM_PROPERTY_FILE_LOC);
            long executionDuration = heronConfig.getLong(LocalHeronDoTask.EXECUTION_DURATION_KEY);
            backtype.storm.utils.Utils.sleep(executionDuration * 1000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        } catch (AlreadyAliveException aae) {
            aae.printStackTrace();
        } catch (InvalidTopologyException ite) {
            ite.printStackTrace();
        } catch (NotAliveException nae) {
            nae.printStackTrace();
        }

    }
}