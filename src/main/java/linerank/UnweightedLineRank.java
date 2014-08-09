package org.apache.giraph.examples.linerank;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class UnweightedLineRank extends BasicComputation<IntWritable,
DoubleWritable, Directions, DoubleWritable>{

	  
	  public static final String NUM_OF_EDGES = "linerank.numofedges";
	  public static final String MAXIMUM_SUPERSTEPS = "maximum.supersteps";
	  
	  public static final double df1 = 0.85;
	  public static final String L1NORM_AGG = "l1norm.aggregator";
	 public static boolean flag = false;
	  public static final double df2 = 0.15;
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
		
		UnweightedLineRankVertexWorkerContext workerContext = getWorkerContext();
		long supersteps= workerContext.getSupersteps();
		double numEdges = (double)workerContext.getNumberofedges();  
	    double state = 0.0;
	    
	    //System.out.println("supersteps-->"+supersteps);
	    //System.out.println("numEdges-->"+numEdges);
	    
	    DoubleWritable sendingVal = new DoubleWritable();
	    int numAdjEdges = numAdjacentEdges(vertex);
	    
	    if (getSuperstep() == 0L) {
	      state = 1/numEdges;
	      vertex.getValue().set(state);
	      sendingVal.set(state/numAdjEdges);
	    } else {
	    	//System.out.println("-------->"+vertex.getId()+"------------------------>");
	    	for (DoubleWritable message : messages) {
	    		//computing current v2 value
	    		//System.out.println(message);
				state += message.get();
		    }
	    	//System.out.println(getSuperstep()+"--->"+state+"    "+numAdjEdges);
	    	double part1 = df1 * state;
	    	double part2= df2/numEdges;
	    	double v2 = part2 + part1;
	    	//computing edge score which is flowing through the messages
	    	double distribute = v2/numAdjEdges;
		    sendingVal.set(distribute);
		   // System.out.println("=========>"+numAdjEdges+"-----------------------------------dis---------------->"+distribute);
	    	 //termination condition 
		    double prev = 0;
	        if(getSuperstep() == 1L){
	        	prev = vertex.getValue().get();
	        	double delta = (Math.abs(prev - v2));
	        	this.aggregate(L1NORM_AGG,new DoubleWritable(delta));
	        	//System.out.println(getSuperstep() +"--previous"+ vertex.getId()+"-->"+prev+"    "+v2);
	        } else{
	      	   prev = vertex.getValue().get()*df1 + part2;
	      	  double delta = (Math.abs(prev - v2)) ;
	      	  this.aggregate(L1NORM_AGG,new DoubleWritable(delta));
	      	  //System.out.println("==>"+delta);
	      	//System.out.println(getSuperstep() +"--previous"+ vertex.getId()+"-->"+prev+"    "+v2);
	        }
	        
	       /* if(getSuperstep() == supersteps){
	        	//System.out.println(prev+"  *   "+numAdjEdges+"  +  "+state);
	        	state = state +(prev*numAdjEdges);
	        	
	        }*/
	    	
	  	 vertex.getValue().set(state);
	    }
	    // System.out.println("v1------------------------->"+vertex.getValue().get());
	    if (getSuperstep() < supersteps) {
	      sendMessageToMultipleEdges(
	          new IncidentVerticesIterator(vertex.getEdges().iterator()),sendingVal);
	    } 
	    else {
	      vertex.voteToHalt();
	    }
	   
	  /*  if (getSuperstep() < supersteps-1) {
	        sendMessageToMultipleEdges(
	            new IncidentVerticesIterator(vertex.getEdges().iterator()),sendingVal);
	      } 
	      if(getSuperstep() == supersteps-1){
	    	  
	    	  double part1 = df1 * state;
	      	double part2= df2/numEdges;
	      	double v2 = part2 + part1;
	      //	System.out.println("sending--->"+v2);
	    	  sendingVal.set(v2);
	      	 sendMessageToMultipleEdges(new IncidentAndAdjacentIterator(vertex.getEdges().iterator()),sendingVal);
	      }
	      else {
	        vertex.voteToHalt();
	      }*/

	  }
	  private int numAdjacentEdges(
	      Vertex<IntWritable, DoubleWritable, Directions> vertex) {
	    int numAdjacentEdges = 0;
	    for (Edge<IntWritable, Directions> edge : vertex.getEdges()) {
	      if (edge.getValue().isAdjacent()) {
	    	  numAdjacentEdges++;
	      }
	    }
	    return numAdjacentEdges;
	  }
	 


}
