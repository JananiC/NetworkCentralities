
package eu.stratosphere.centrality.closeness;

import java.util.Iterator;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.accumulators.IntCounter;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.centrality.utils.CentralityUtil;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.util.Collector;

/**
Name : Effective Closeness <br>
Description : Finds closeness of a node with respect to all other nodes <br>
Reference : Centralities in Large Networks: Algorithms and Observations-U Kang
<p>
Closeness Definition: Inverse of the average of shortest
					  distances to all other nodes.
Implemented by using delta-iteration operator in stratosphere
and the corresponding data-flow follows three parts, <br>
</p>
<ul>
<li>Reads input files vertices <vertexId> and edges
<srcVertexId, targetVertexId> <br>

<li>Computes the summation part (of average computation based on
 "Effective Closeness algorithm" and Flajolet-Martin) for each vertex
 iteratively and the final output format is  <vertexId, bit[], sum> <br>

<li>Computes inverse of the average ((n-1)/sum) for each vertex using
the summation from the second step and the final output format
is <vertexId, closeness> <br>
</ul>
@Parameters [Degree of parallelism],[vertices input-path],[edge
input-path], [out-put],[Max-Num of iterations],[Number of vertices]
 */
/**
 * 
 * @author JANANI
 *
 */
@SuppressWarnings("serial")
public class EffectiveCloseness {

	public static void main(String[] args) throws Exception {
		
		String fieldDelimiter = CentralityUtil.TAB_DELIM;
		int keyPosition = 0;
		
		if (args.length < 6) {
			System.err.println("Usage:[Degree of parallelism],[vertices input-path],"
					+ "[edge input-path],[out-put],[Max-Num of iterations],[Number of vertices]"
					+ ",[Delimiter]");
			return;
		}

		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]): 1);
		final String verticesInput = (args.length > 1 ? args[1] : "");
		final String edgeInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int maxIterations = (args.length > 4 ? Integer.parseInt(args[4]): 5);
		final String numVertices = (args.length > 5 ? (args[5]) : CentralityUtil.ZERO);
		
		if(args.length>6){
			fieldDelimiter = (args[6]);
		}
	
	    char delim = CentralityUtil.checkDelim(fieldDelimiter);

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setDegreeOfParallelism(numSubTasks);
		
		DataSet<Tuple3<Long, FMCounter, Double>> initialSolutionSet = env
				.readCsvFile(verticesInput).lineDelimiter(CentralityUtil.NEWLINE)
				.types(Long.class).map(new AssignBitArrayToVertices())
				.name("Assign BitArray to Vertex");
		/**
		 * . The initial vertices input file is set as both work-set and
		 * solution-set
		 */
		DataSet<Tuple3<Long, FMCounter, Double>> initialworkingSet = initialSolutionSet;

		/**
		 * Reads Edges input file of the format <sourceId, destinationId>
		 */
		DataSet<Tuple2<Long, Long>> edgeSet = env.readCsvFile(edgeInput).fieldDelimiter(delim).types(Long.class, Long.class);

	
		/**
		 * =Part2:=* Initializing delta-iteration
		 */
		DeltaIteration<Tuple3<Long, FMCounter, Double>,
		Tuple3<Long, FMCounter, Double>> deltaIteration = initialSolutionSet.iterateDelta(initialworkingSet, maxIterations, keyPosition);
		//deltaIteration.name("Effective Closeness Iteration");
		
	
		DataSet<Tuple3<Long, FMCounter, Double>> candidateUpdates = deltaIteration
				.getWorkset().join(edgeSet).where(0).equalTo(1).with(new SendingMessageToNeighbors()).name("Sending Message To Neighbors")
				.groupBy(0).reduceGroup(new PartialBitwiseOR()).name("BitwiseOR")
				.join(deltaIteration.getSolutionSet()).where(0).equalTo(0)
				.with(new FilterConvergedNodes()).name("Filters Converged Vertices");
		/**
		 * Termination condition for the iteration :- when the work-set of
		 * previous iteration and current iteration are same
		 */
		

		DataSet<Tuple3<Long, FMCounter, Double>> finalSolnSet = deltaIteration
					.closeWith(candidateUpdates, candidateUpdates);
		/**
		 * =Part3:= Computation of average with a single map which reads the
		 * output<vertexId,sum> of the iteration
		 */
		DataSet<Tuple2<Long, Double>> closeness = finalSolnSet
			.map(new AverageComputation(numVertices)).name("Average Computation");
		/**
		 * Data Sink: Writing results back to disk
		 */
		closeness.writeAsCsv(output, CentralityUtil.NEWLINE, CentralityUtil.TAB_DELIM, WriteMode.OVERWRITE);
		JobExecutionResult job = env.execute();
		
		System.out.println("Total number of iterations-->"+((job.getIntCounterResult(SendingMessageToNeighbors.ACCUM_LOCAL_ITERATIONS)/numSubTasks)+1));
		System.out.println("RunTime-->"+ (job.getNetRuntime()));
	}
	@ConstantFields("0")
	public static final class AverageComputation extends
		MapFunction<Tuple3<Long, FMCounter, Double>,
		Tuple2<Long, Double>> {
		Long noOfver;
		public AverageComputation(String numVer) {
			this.noOfver = Long.parseLong(numVer);
		}
		Double closeness = 0.0;
		@Override
		public Tuple2<Long, Double> map(
			Tuple3<Long, FMCounter, Double> value)
			throws Exception {
			
			if (value.f2 > 0) {
			closeness = value.f2 /(noOfver-1) ;
			}
			Tuple2<Long, Double> emitcloseness = new Tuple2<Long, Double>();
			emitcloseness.f0 = value.f0;
			emitcloseness.f1 = closeness;
			return emitcloseness;
		}

	}
	@ConstantFields("0")
	public static final class AssignBitArrayToVertices extends
		MapFunction<Tuple1<Long>, Tuple3<Long, FMCounter, Double>> {
		@Override
		public Tuple3<Long, FMCounter, Double> map(Tuple1<Long> value)
			throws Exception {
			FMCounter counter = new FMCounter();
			counter.addNode(value.f0.intValue());
			Tuple3<Long,FMCounter, Double> result = new Tuple3<Long,FMCounter, Double>();
			result.f0 = value.f0;
			result.f1 = counter;
			result.f2 = 0.0;
			return result;
		}
	}
	@ConstantFieldsFirst("1->1;2 -> 2")
	 @ConstantFieldsSecond("0 -> 0")
	public static final class SendingMessageToNeighbors
		extends
		JoinFunction<Tuple3<Long, FMCounter, Double>,
		Tuple2<Long, Long>, Tuple3<Long, FMCounter, Double>> {
		public static final String ACCUM_LOCAL_ITERATIONS = "accum.local.iterations";
		private IntCounter localIterations = new IntCounter();
		
		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(ACCUM_LOCAL_ITERATIONS,
					localIterations);
			localIterations.add(1);
		}
		@Override
		public Tuple3<Long, FMCounter, Double> join(Tuple3<Long, FMCounter, Double> vertex_workset,Tuple2<Long, Long> neighbors) throws Exception {

			Tuple3<Long, FMCounter, Double> output = new Tuple3<Long, FMCounter, Double>();
			output.f0 = neighbors.f0;
			output.f1 = vertex_workset.f1;
			output.f2 = vertex_workset.f2;
			//System.out.println("Joining at des id -->"+neighbors.f1+" distributes to only needed vertices which becomes partial bitstring of --->"+neighbors.f0);
			return output;
		}
	}
	@eu.stratosphere.api.java.functions.GroupReduceFunction.Combinable
	public static final class PartialBitwiseOR
		extends
		GroupReduceFunction<Tuple3<Long, FMCounter, Double>,
		Tuple3<Long, FMCounter, Double>> {
		@Override
		public void reduce(Iterator<Tuple3<Long, FMCounter, Double>> values,Collector<Tuple3<Long, FMCounter, Double>> out) throws Exception {
			Tuple3<Long, FMCounter, Double> first = values.next();
			Long ver = first.f0;
			Double sum = first.f2;
			FMCounter counter = first.f1.copy();
			while (values.hasNext()) {
				counter.merge(values.next().f1);
			}
			//System.out.println("Getting all bitstrings of adjacent vertices of "+ver+" and its count now --->"+counter.getCount()+"   at step "+getIterationRuntimeContext().getSuperstepNumber());
			
			Tuple3<Long,FMCounter, Double> result = new Tuple3<Long,FMCounter, Double>();
			result.f0 = ver;
			result.f1 = counter;
			result.f2  = sum;
			out.collect(result); 
		}
		@Override
        public void combine(
    			Iterator<Tuple3<Long, FMCounter, Double>> values,
    			Collector<Tuple3<Long, FMCounter, Double>> out)
    			throws Exception {
			Tuple3<Long, FMCounter, Double> first = values.next();
			Long ver = first.f0;
			Double sum = first.f2;
			FMCounter counter = first.f1.copy();
			while (values.hasNext()) {
				counter.merge(values.next().f1);
			}
			//System.out.println("Getting all bitstrings of adjacent vertices of "+ver+" and its count now --->"+counter.getCount()+"   at step "+getIterationRuntimeContext().getSuperstepNumber());
			
			Tuple3<Long,FMCounter, Double> result = new Tuple3<Long,FMCounter, Double>();
			result.f0 = ver;
			result.f1 = counter;
			result.f2  = sum;
			out.collect(result); 
		}
	}

	public static final class FilterConvergedNodes extends JoinFunction<Tuple3<Long, FMCounter, Double>,Tuple3<Long, FMCounter, Double>,Tuple3<Long, FMCounter, Double>>{
		long NNOfNode_before;
		long NNofNode_After;
		long diff;
		Double sum;
		long iterationNumber;
		//List<Long> worksetSize = new ArrayList<Long>();
		
		@Override
		public void open(Configuration parameters) throws Exception {
			if(getIterationRuntimeContext().getSuperstepNumber()!=1L){
			//System.out.println("Current Workset size--->"+worksetSize.size()+" at iteration "+getIterationRuntimeContext().getSuperstepNumber());
			//worksetSize.clear();
			}
		}
		@Override
		public void join(Tuple3<Long, FMCounter, Double> current, Tuple3<Long, FMCounter, Double> previous, Collector<Tuple3<Long, FMCounter, Double>> out) {
		
			iterationNumber = getIterationRuntimeContext().getSuperstepNumber();
			
			FMCounter prevFm = previous.f1;
			FMCounter currFm = current.f1;

			NNOfNode_before = prevFm.getCount();
			currFm.merge(prevFm);
			NNofNode_After = currFm.getCount();
			if (NNOfNode_before != NNofNode_After) {
				sum = previous.f2;
				diff = NNofNode_After - NNOfNode_before;
				sum = sum + iterationNumber * (diff);
				current.f2 = sum;
				out.collect(current);
				//worksetSize.add(current.f0);
				// System.out.println("not converged "+current.f0+"  at  "+iterationNumber);
			} else {
				// System.out.println("converged "+current.f0+"  at  "+iterationNumber);
			}
		}
		@Override
		public Tuple3<Long, FMCounter, Double> join(
				Tuple3<Long, FMCounter, Double> current,
				Tuple3<Long, FMCounter, Double> previous) throws Exception {
				
			return null;
		}
	}
	
}



