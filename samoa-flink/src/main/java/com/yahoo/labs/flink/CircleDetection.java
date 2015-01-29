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

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.Stack;

public class CircleDetection {
    private int[] index ;
    private int[] lowLink;
    private int counter ;
    private Stack<Integer> stack;
    private List<List<Integer>> scc;
    List<Integer>[] graph;


    public CircleDetection(){
        stack = new Stack<Integer>();
        scc = new ArrayList<>();
    }

    public List<List<Integer>> getCircles (List<Integer>[] adjacencyList){
        graph = adjacencyList;
        index = new int[adjacencyList.length];
        lowLink = new int[adjacencyList.length];
        counter = 0;

        //initialize index and lowLink as "undefined"(=-1)
        for (int j=0;j<graph.length;j++){
            index[j] = -1;
            lowLink[j]= -1;
        }
        for (int v=0;v<graph.length;v++)
        {
            if (index[v]==-1){ //undefined.
                findSCC(v);
            }
        }
        return scc;
    }

    private void findSCC(int node){
        index[node] = counter;
        lowLink[node] = counter;
        counter++;
        stack.push(node);

        for (int neighbor : graph[node]){
            if (index[neighbor]==-1) {
                findSCC(neighbor);
                lowLink[node] = Math.min(lowLink[node], lowLink[neighbor]);
            }
            else if (stack.contains(neighbor)){ //if neighbor has been already visited
                lowLink[node] = Math.min(lowLink[node], index[neighbor]);
                List<Integer> sccComponent = new ArrayList<Integer>();
                int w;
                do {
                    w = stack.pop();
                    sccComponent.add(w);
                }while(neighbor!=w);
                stack.add(neighbor); //add neighbor again, just in case it is in another circle also
                scc.add(sccComponent);
            }

        }
        if (lowLink[node] == index[node]){
            int w;
            do {
                w = stack.pop();
            }while(node!=w);
        }
    }

    public static void main(String[] args){

        Scanner scan = new Scanner(System.in);
        System.out.println("Enter number of Vertices");
        /* number of vertices */
        int V = scan.nextInt();

        /* make graph */
        List<Integer>[] g = new List[V];
        for (int i = 0; i < V; i++)
            g[i] = new ArrayList<Integer>();
        /* accpet all edges */
        System.out.println("\nEnter number of edges");
        int E = scan.nextInt();
        /* all edges */
        System.out.println("Enter "+ E +" x, y coordinates");
        for (int i = 0; i < E; i++)
        {
            int x = scan.nextInt();
            int y = scan.nextInt();
            g[x].add(y);
        }

        CircleDetection detCircles = new CircleDetection();
        List<List<Integer>> circles = detCircles.getCircles(g); //detect circles in the topology
        System.out.println("SCC:\n"+ circles);
    }

}