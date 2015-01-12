package com.yahoo.labs.flink;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2015 Yahoo! Inc.
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

import com.github.javacliparser.ClassOption;
import com.yahoo.labs.flink.topology.impl.FlinkComponentFactory;
import com.yahoo.labs.samoa.tasks.Task;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Main class to run a SAMOA on Apache Flink
 */
public class FlinkDoTask {

	private static final Logger logger = LoggerFactory.getLogger(FlinkDoTask.class);

	private static final String LOCAL_MODE = "local";
	private static final String REMOTE_MODE = "remote";

	// FLAGS
	private static final String MODE_FLAG = "--mode";
	private static final String DEFAULT_PARALLELISM = "--parallelism";

	//config values
	private static boolean isLocal = true;
	private static String flinkMaster;
	private static int flinkPort;
	private static String[] dependecyJars;
	private static int parallelism = 1;

	public static void main(String[] args) throws Exception {
		List<String> tmpArgs = new ArrayList<String>(Arrays.asList(args));
		extractFlinkArguments(tmpArgs);

		args = tmpArgs.toArray(new String[0]);

		// Init Task
		StringBuilder cliString = new StringBuilder();
		for (int i = 0; i < args.length; i++) {
			cliString.append(" ").append(args[i]);
		}
		logger.debug("Command line string = {}", cliString.toString());
		System.out.println("Command line string = " + cliString.toString());

		Task task = null;
		try {
			task = (Task) ClassOption.cliStringToObject(cliString.toString(), Task.class, null);
			logger.info("Sucessfully instantiating {}", task.getClass().getCanonicalName());
		} catch (Exception e) {
			logger.error("Fail to initialize the task", e);
			System.out.println("Fail to initialize the task" + e);
			return;
		}

		StreamExecutionEnvironment env = (isLocal) ? StreamExecutionEnvironment.createLocalEnvironment() :
				StreamExecutionEnvironment.createRemoteEnvironment(flinkMaster, flinkPort, parallelism, dependecyJars);

		task.setFactory(new FlinkComponentFactory(env));
		task.init();
		env.execute();

	}

	private static void extractFlinkArguments(List<String> tmpargs) {
		for (int i = tmpargs.size() - 1; i >= 0; i--) {
			String arg = tmpargs.get(i).trim();
			String[] splitted = arg.split("=", 2);

			if (splitted.length >= 2) {
				if (MODE_FLAG.equals(splitted[0])) {
					isLocal = LOCAL_MODE.equals(splitted[1]);
					tmpargs.remove(i);
				}
			}


		}
	}

}
