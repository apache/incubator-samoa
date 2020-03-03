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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.Config;
import com.twitter.heron.api.exception.AlreadyAliveException;
import com.twitter.heron.api.exception.InvalidTopologyException;
import com.twitter.heron.api.utils.Utils;

/**
 * Helper class to submit SAMOA task into heron without the need of submitting the jar file. The jar file must be
 * submitted first using HeronJarSubmitter class.
 *
 * @author Arinto Murdopo
 */
public class HeronTopologySubmitter {

    public static String YJP_OPTIONS_KEY = "YjpOptions";

    private static Logger logger = LoggerFactory.getLogger(HeronTopologySubmitter.class);

    public static void main(String[] args) throws IOException {
        Properties props = HeronSamoaUtils.getProperties();

        String uploadedJarLocation = props.getProperty(HeronJarSubmitter.UPLOADED_JAR_LOCATION_KEY);
        if (uploadedJarLocation == null) {
            logger.error("Invalid properties file. It must have key {}",
                    HeronJarSubmitter.UPLOADED_JAR_LOCATION_KEY);
            return;
        }

        List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));
        int numWorkers = HeronSamoaUtils.numWorkers(tmpArgs);

        args = tmpArgs.toArray(new String[0]);
        HeronTopology heronTopo = HeronSamoaUtils.argsToTopology(args);

        Config conf = new Config();
        //conf.putAll(Utils.readStormConfig());
        conf.putAll(Utils.readCommandLineOpts());
        conf.setDebug(false);
        conf.setNumStmgrs(numWorkers);

        String profilerOption =
                props.getProperty(HeronTopologySubmitter.YJP_OPTIONS_KEY);
        if (profilerOption != null) {
            String topoWorkerChildOpts = (String) conf.get(Config.TOPOLOGY_WORKER_CHILDOPTS);
            StringBuilder optionBuilder = new StringBuilder();
            if (topoWorkerChildOpts != null) {
                optionBuilder.append(topoWorkerChildOpts);
                optionBuilder.append(' ');
            }
            optionBuilder.append(profilerOption);
            conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, optionBuilder.toString());
        }

        Map<String, Object> myConfigMap = new HashMap<String, Object>(conf);
        StringWriter out = new StringWriter();
        String topologyName = heronTopo.getTopologyName();

        try {
            JSONValue.writeJSONString(myConfigMap, out);


            Config config = new Config();


            System.out.println("Submitting topology with name: "
                    + topologyName);
            HeronSubmitter.submitTopology(topologyName, conf, heronTopo.getHeronBuilder().createTopology());
            System.out.println(topologyName + " is successfully submitted");
        } catch (IOException e) {
            System.out.println("Error in writing JSONString");
            e.printStackTrace();
            return;
        } catch (AlreadyAliveException aae) {
            aae.printStackTrace();
        } catch (InvalidTopologyException ite) {
            System.out.println("Invalid topology for " + topologyName);
            ite.printStackTrace();
        }
    }

    private static String uploadedJarLocation(List<String> tmpArgs) {
        int position = tmpArgs.size() - 1;
        String uploadedJarLocation = tmpArgs.get(position);
        tmpArgs.remove(position);
        return uploadedJarLocation;
    }
}
