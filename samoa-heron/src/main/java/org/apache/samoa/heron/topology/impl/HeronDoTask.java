package org.apache.samoa.heron.topology.impl;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
//import com.twitter.heron.api.exception.NotAliveException;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.simulator.Simulator;

/**
 * The main class that used by samoa script to execute SAMOA task.
 *
 * @author Arinto Murdopo
 */
public class HeronDoTask {
    private static final Logger logger = LoggerFactory.getLogger(HeronDoTask.class);
    private static String localFlag = "local";
    private static String clusterFlag = "cluster";

    /**
     * The main method.
     *
     * @param args the arguments
     */
    public static void main(String[] args) throws AlreadyAliveException {

        List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));

        boolean isLocal = isLocal(tmpArgs);
        int numWorker = HeronSamoaUtils.numWorkers(tmpArgs);

        args = tmpArgs.toArray(new String[0]);

        // convert the arguments into Storm topology
        HeronTopology heronTopo = HeronSamoaUtils.argsToTopology(args);
        String topologyName = heronTopo.getTopologyName();

        Config conf = new Config();
        //conf.putAll(Utils.readStormConfig());
        conf.setDebug(false);

        try {
            if (isLocal) {
                // local mode
                //conf.setMaxTaskParallelism(numWorker);
                Simulator cluster = new Simulator();
                cluster.submitTopology(topologyName, conf, heronTopo.getHeronBuilder().createTopology());
                backtype.storm.utils.Utils.sleep(600 * 1000);
                cluster.killTopology(topologyName);
                cluster.shutdown();
            } else {
                // cluster mode
                conf.setNumStmgrs(numWorker);
                HeronSubmitter.submitTopology(topologyName, conf,
                        heronTopo.getHeronBuilder().createTopology());

            }
        } catch (AlreadyAliveException aae) {
            aae.printStackTrace();
        } catch (InvalidTopologyException ite) {
            ite.printStackTrace();
        }
    }

    private static boolean isLocal(List<String> tmpArgs) {
        ExecutionMode executionMode = ExecutionMode.UNDETERMINED;

        int position = tmpArgs.size() - 1;
        String flag = tmpArgs.get(position);
        boolean isLocal = true;

        if (flag.equals(clusterFlag)) {
            executionMode = ExecutionMode.CLUSTER;
            isLocal = false;
        } else if (flag.equals(localFlag)) {
            executionMode = ExecutionMode.LOCAL;
            isLocal = true;
        }

        if (executionMode != ExecutionMode.UNDETERMINED) {
            tmpArgs.remove(position);
        }

        return isLocal;
    }

    private enum ExecutionMode {
        LOCAL, CLUSTER, UNDETERMINED
    }

    ;
}
