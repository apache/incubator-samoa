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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.yahoo.labs.flink.topology.impl.FlinkComponentFactory;
import com.yahoo.labs.flink.topology.impl.FlinkProcessingItem;
import com.yahoo.labs.flink.topology.impl.FlinkStream;
import com.yahoo.labs.flink.topology.impl.FlinkTopology;
import com.yahoo.labs.samoa.tasks.Task;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
		Utils.extractFlinkArguments(tmpArgs);

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
			logger.info("Successfully instantiating {}", task.getClass().getCanonicalName());
		} catch (Exception e) {
			logger.error("Fail to initialize the task: ", e);
			System.out.println("Fail to initialize the task: " + e);
			return;
		}

		StreamExecutionEnvironment env = (Utils.isLocal) ? StreamExecutionEnvironment.createLocalEnvironment(Utils.parallelism) :
				StreamExecutionEnvironment.createRemoteEnvironment(Utils.flinkMaster, Utils.flinkPort, Utils.parallelism, Utils.dependecyJars);

		logger.info("Creating the factory\n");
		task.setFactory(new FlinkComponentFactory(env));

		logger.info("Going to initialize the task\n");
		task.init();

		List<List<Integer>> circles = extractTopologyGraph((FlinkTopology) task.getTopology());
		System.out.println("Circles found in the graph: " + circles);

		logger.info("Going to build the topology\n");
		((FlinkTopology) task.getTopology()).build();

		logger.info("Execute environment\n");
		env.execute();

	}

	private static List<List<Integer>> extractTopologyGraph(FlinkTopology topology){
		List<FlinkProcessingItem> pis = Lists.newArrayList(Iterables.filter(topology.getProcessingItems(), FlinkProcessingItem.class));
		List<Integer>[] graph = new List[pis.size()];
		List<String>[] inStreams = new ArrayList[pis.size()];
		Map<String,Integer> outStreams = new HashMap<>();

		for (int i=0;i<pis.size();i++) {
			graph[i] = new ArrayList<Integer>();
			inStreams[i] = new ArrayList<String>();
		}

		for (FlinkProcessingItem pi: pis){
			//get output streams of the processing items: <SteamId,sourcePI>
			for (FlinkStream os :  pi.getOutputStreams()) {
				outStreams.put(os.getStreamId(), pi.getPiID());
			}
			//get input streams of the processing items: [PI]-> [streamId1,streamId2,...]
			for (Tuple2<FlinkStream,Utils.Partitioning> is :  pi.getInputStreams()) {
				inStreams[pi.getPiID()].add(is.f0.getStreamId());
			}
		}

		int node ;
		for (int i=0;i<pis.size();i++){ //for each processing item(PI)
			for (int j=0;j<inStreams[i].size();j++){ // for all input streams to a PI
				if (outStreams.containsKey(inStreams[i].get(j))){ //if it doesn't come from an Entrance PI
					node = outStreams.get(inStreams[i].get(j)); // the source node of the input stream
					graph[node].add(i);                        // has this PI as neighbor
				}
			}
		}
		for (int g=0;g<graph.length;g++)
			System.out.println(graph[g].toString());

		CircleDetection detCircles = new CircleDetection();
		List<List<Integer>> circles = detCircles.getCircles(graph); //detect circles in the topology

		return circles;
	}

}
