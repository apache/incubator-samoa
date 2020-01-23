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

package org.apache.samoa.flink;

import com.github.javacliparser.ClassOption;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.samoa.flink.topology.impl.FlinkComponentFactory;
import org.apache.samoa.flink.topology.impl.FlinkTopology;
import org.apache.samoa.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Main class to run a SAMOA on Apache Flink
 */
public class FlinkDoTask {

	private static final Logger logger = LoggerFactory.getLogger(FlinkDoTask.class);


	public static void main(String[] args) throws Exception {
		List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));

		args = tmpArgs.toArray(new String[0]);

		// Init Task
		StringBuilder cliString = new StringBuilder();
		for (int i = 0; i < args.length; i++) {
			cliString.append(" ").append(args[i]);
		}
		logger.debug("Command line string = {}", cliString.toString());
		System.out.println("Command line string = " + cliString.toString());

		Task task;
		try {
			task = ClassOption.cliStringToObject(cliString.toString(), Task.class, null);
			logger.debug("Successfully instantiating {}", task.getClass().getCanonicalName());
		} catch (Exception e) {
			logger.error("Failed to initialize the task: ", e);
			System.out.println("Failed to initialize the task: " + e);
			return;
		}
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		task.setFactory(new FlinkComponentFactory(env));
		task.init();
		
		logger.debug("Building Flink topology...");
		((FlinkTopology) task.getTopology()).build();
		
		logger.debug("Submitting the job...");
		env.execute();

	}


	

}
