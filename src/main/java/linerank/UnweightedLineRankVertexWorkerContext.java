package org.apache.giraph.examples.linerank;

import org.apache.giraph.worker.WorkerContext;

public class UnweightedLineRankVertexWorkerContext extends WorkerContext {
	/** Number of super steps to run (10 by default) */
	private long supersteps = 10L;
	/** Total number of edges in the graph(20 by default) */
	private long numberofedges = 20L;

	@Override
	public void preApplication() throws InstantiationException, IllegalAccessException {
		supersteps = getContext().getConfiguration().getLong(UnweightedLineRank.MAXIMUM_SUPERSTEPS, supersteps);
		numberofedges = getContext().getConfiguration().getLong(UnweightedLineRank.NUM_OF_EDGES, numberofedges);
	}
	public long getSupersteps() {
		return supersteps;
	}
	public long getNumberofedges() {
		return numberofedges;
	}
	@Override
	public void postApplication() {
	}
	@Override
	public void preSuperstep() {
	}
	@Override
	public void postSuperstep() { 
	}
	
}
