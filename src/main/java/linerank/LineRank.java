/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples.linerank;


import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.linerank.Directions;
import org.apache.giraph.examples.linerank.IncidentAndAdjacentIterator;
import org.apache.giraph.examples.linerank.IncidentVerticesIterator;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.utils.MathUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class LineRank extends BasicComputation<IntWritable,
    DoubleWritable, Directions, DoubleWritable> {

  private static final long NUM_EDGES = 5;
  public static int MAX_SUPERSTEPS = 20;
 // float dampingFactor = this.getConf().getFloat("damping.factor", 0.85f);
  public static final double c = 0.85;
  public static final String L1NORM_AGG = "l1norm.aggregator";
  public static  boolean flag = false;
  public static final double rdf= 0.15/NUM_EDGES;
  
	
	/**
	 * v1 <- initializing v1; 
		while until v1 converges
		v2 <- S(G)T v1;
		v3 <- T(G)v2;
		v1 <- cv3+(1-c)r; 
	 */

  @Override
  public void compute(Vertex<IntWritable, DoubleWritable, Directions> vertex,
                      Iterable<DoubleWritable> messages) throws IOException {
    double state = 0;
    DoubleWritable sendingVal = new DoubleWritable();
    int numIncEdges = numIncidentEdges(vertex);
    if (getSuperstep() == 0L) {
      state = 1.0 / NUM_EDGES;
      vertex.getValue().set(state);
      double send = state;
      sendingVal.set(send);
      
    } else {
    	if(getSuperstep() == MAX_SUPERSTEPS){
    		for (DoubleWritable message : messages) {
    			state += message.get();
        	}
    	  double prev = vertex.getValue().get()*numIncEdges;
          System.out.println("prev --> "+prev);
          System.out.println("current step -->"+state);
          double add = prev+state;
          System.out.println("Total-->"+add);
          vertex.getValue().set(add);
    	}
    	else {
    		for (DoubleWritable message : messages) {
    			state += message.get();
        	}
          double normalized = state/numIncEdges;
          double firstPart = c * normalized;
          double v2 = rdf + firstPart;
          double send = v2 ;
          sendingVal.set(send);
         
          System.out.println("v2 ------------------------------------------====>"+send);
          
          //termination condition 
          if(getSuperstep() == 1L){
        	  double prev = vertex.getValue().get();
        	  double delta = (Math.abs(prev - send)) * numIncEdges;
        	  this.aggregate(L1NORM_AGG,new DoubleWritable(delta));
          } else{
        	  double prev = vertex.getValue().get()*c + rdf;
        	  double delta = (Math.abs(prev - send)) * numIncEdges;
        	  this.aggregate(L1NORM_AGG,new DoubleWritable(delta));
          }
      	 vertex.getValue().set(normalized);
    	}
    	
    }
    System.out.println("v1------------------------->"+vertex.getValue().get());
    System.out.println("final flag------->"+flag);
    if (getSuperstep() < MAX_SUPERSTEPS-1) {
      sendMessageToMultipleEdges(
          new IncidentVerticesIterator(vertex.getEdges().iterator()),sendingVal);
    } 
    if(getSuperstep() == MAX_SUPERSTEPS-1){
    	 sendMessageToMultipleEdges(new IncidentAndAdjacentIterator(vertex.getEdges().iterator()),sendingVal);
    }
    else {
      vertex.voteToHalt();
    }

  }


  private int numIncidentEdges(
      Vertex<IntWritable, DoubleWritable, Directions> vertex) {
    int numIncidentEdges = 0;
    for (Edge<IntWritable, Directions> edge : vertex.getEdges()) {
      if (edge.getValue().isIncident()) {
        numIncidentEdges++;
      }
    }
    return numIncidentEdges;
  }
  
  
  public static class LineRankMaster extends DefaultMasterCompute {
		public static final double EPSILON = 0.00001;
	    @Override
	    public void compute() {
	    	
	    	long step = getSuperstep();
	    	if (step >1){
	    	double maxDelta = ((DoubleWritable) getAggregatedValue(L1NORM_AGG)).get();
	    	if (maxDelta < EPSILON){
	    		flag = true;
	    	//haltComputation();
	    	System.out.println(maxDelta+" < "+EPSILON+" halting computation  "+step);
	    	}
	    	}
	    	
	    	
	    }
	  @Override
	    public void initialize() throws InstantiationException,
	        IllegalAccessException {
		  registerAggregator(L1NORM_AGG,DoubleSumAggregator.class);
	    }
  }

}
