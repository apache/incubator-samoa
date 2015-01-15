package com.yahoo.labs.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class CircleDetection {
    private int[] index ;
    private int[] lowLink;
    private Stack<Integer> stack;
    private List<List<Integer>> scc;
    List<Integer>[] graph;
    private int counter ;

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
        for (int j=0;j<adjacencyList.length;j++){
            index[j] = -1;
            lowLink[j]= -1;
        }
        for (int v=0;v<adjacencyList.length;v++)
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

        //search successors of node
        for (int neighbor : graph[node]){
            if (index[neighbor]==-1) {
                findSCC(neighbor);
                lowLink[node] = Math.min(lowLink[node], lowLink[neighbor]);
            }
            else if (stack.contains(neighbor)){
                lowLink[node] = Math.min(lowLink[node], index[neighbor]);
            }
        }
        //If node is a root node, pop stack and generate the SCC
        List<Integer> sccComponent = new ArrayList<Integer>();
        if (lowLink[node] == index[node]){
            int w;
            do {
                w = stack.pop();
                sccComponent.add(w);
            }while(node!=w);
            //reject here scc that have size=1 //rethink that in case we need also scc of size=1
            if (sccComponent.size()>1) scc.add(sccComponent);
        }
    }
}