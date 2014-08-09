package org.apache.giraph.examples.linerank;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;

public class LineRankMaster extends DefaultMasterCompute {
	//public static boolean flag = false;
	public static final double EPSILON = 0.00001;
    @Override
    public void compute() {
    long step = getSuperstep();
    if (step >1){
    	double maxDelta = ((DoubleWritable) getAggregatedValue(UnweightedLineRank.L1NORM_AGG)).get();
    	if (maxDelta < EPSILON){
    		//UnweightedLineRank.supersteps = step+1;
    		//flag = true;
    	haltComputation();
    	System.out.println(maxDelta+" < "+EPSILON+" Halting computation  at step===>"+step);
    	}
    }
    	
    	
    }
  @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
	  registerAggregator(UnweightedLineRank.L1NORM_AGG,DoubleSumAggregator.class);
    }

}
