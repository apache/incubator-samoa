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
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Main class to run a SAMOA on Apache Flink
 */
public class FlinkDoTask {

	private static final Logger logger = LoggerFactory.getLogger(FlinkDoTask.class);
	public static List<List<FlinkProcessingItem>> circles ;


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

		circles = extractTopologyGraph((FlinkTopology) task.getTopology());

		logger.info("Going to build the topology\n");
		((FlinkTopology) task.getTopology()).build();

		logger.info("Execute environment\n");
		env.execute();

	}

	private static List<List<FlinkProcessingItem>> extractTopologyGraph(FlinkTopology topology){
		List<FlinkProcessingItem> pis = Lists.newArrayList(Iterables.filter(topology.getProcessingItems(), FlinkProcessingItem.class));
		List<Integer>[] graph = new List[pis.size()];
		FlinkProcessingItem[] processingItems = new FlinkProcessingItem[pis.size()];
		List<List<FlinkProcessingItem>> piCircles = new ArrayList<>();


		for (int i=0;i<pis.size();i++) {
			graph[i] = new ArrayList<Integer>();
		}
		//construct the graph of the topology for the Processing Items (No entrance pi is included)
		for (FlinkProcessingItem pi: pis) {
			processingItems[pi.getId()] = pi; //ordered processing items
			for (Tuple3<FlinkStream, Utils.Partitioning, Integer> is : pi.getInputStreams()) {
				if (is.f2 != -1) graph[is.f2].add(pi.getId());
			}
		}
		for (int g=0;g<graph.length;g++)
			System.out.println(graph[g].toString());

		CircleDetection detCircles = new CircleDetection();
		List<List<Integer>> circles = detCircles.getCircles(graph); //detect circles in the topology

		//update PIs, regarding being part fo a circle.
		for (List<Integer> c : circles){
			List<FlinkProcessingItem> circle = new ArrayList<>();
			for (Integer it : c){
				circle.add(processingItems[it]); //add processing Item in the circle
				processingItems[it].setPartOfCircle(true);
				processingItems[it].setCircleId(piCircles.size()); //set the Id of the circle that this PI belongs to
				//System.out.println("piId: "+processingItems[it].getPiID());
			}
			piCircles.add(circle);
		}
		System.out.println("Circles in the topology: " +circles);

		return piCircles;
	}

}
