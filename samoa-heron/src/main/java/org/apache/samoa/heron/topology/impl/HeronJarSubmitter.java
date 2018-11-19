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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.utils.Utils;

/**
 * Utility class to submit samoa-storm jar to a Heron cluster.
 *
 * @author Arinto Murdopo
 */
public class HeronJarSubmitter {

    public final static String UPLOADED_JAR_LOCATION_KEY = "UploadedJarLocation";

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {


        //TODO refactor this whole code to use submitTopology instead
        //as submitJar is not longer available in heron-storm
    /*Map config = Utils.readStormConfig();
    //config.putAll(Utils.readCommandLineOpts());


    System.out.println("uploading jar from " + args[0]);
    String uploadedJarLocation = StormSubmitter.submitJar(config, args[0]);

    System.out.println("Uploaded jar file location: ");
    System.out.println(uploadedJarLocation);

    Properties props = HeronSamoaUtils.getProperties();
    props.setProperty(HeronJarSubmitter.UPLOADED_JAR_LOCATION_KEY, uploadedJarLocation);

    File f = new File("src/main/resources/samoa-heron-cluster.properties");
    f.createNewFile();

    OutputStream out = new FileOutputStream(f);
    props.store(out, "properties file to store uploaded jar location from HeronJarSubmitter");*/
    }
}
