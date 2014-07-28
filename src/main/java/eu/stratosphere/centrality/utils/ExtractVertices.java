package eu.stratosphere.centrality.utils;

import java.util.Iterator;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.accumulators.IntCounter;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.util.Collector;
/**
 * 
 * @author JANANI
 *
 */
public class ExtractVertices {

	
	public static void main(String[] args) throws Exception {

		if (args.length < 3) {
			System.err.println("Usage:[Degree of parallelism],[edge input-path],[out-put],[delimiter]");
			return;
		}
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0])
				: 1);
		final String inputfilepath = (args.length > 1 ? args[1] : "");
		final String outputfilepath = (args.length > 2 ? args[2] : "");
	     String fieldDelimiter = " ";
	        if(args.length>3){
	        	fieldDelimiter = (args[3]);
	        }
	    char delim = CentralityUtil.checkDelim(fieldDelimiter);
	        
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		env.setDegreeOfParallelism(numSubTasks);
		
		DataSet<Tuple1<Long>> first = env.readCsvFile(inputfilepath).fieldDelimiter(delim)
		.types(Long.class);
		
		DataSet<Long> result =  first.groupBy(0).reduceGroup(new Reducer());
		result.writeAsText(outputfilepath, WriteMode.OVERWRITE);
		JobExecutionResult job = env.execute();
		System.out.println("Total number of vertices-->"+(job.getIntCounterResult(Reducer.TOTAL_VERTICES)));
		System.out.println("RunTime-->"+ ((job.getNetRuntime()/1000))+"sec");
		
	}

	
	@eu.stratosphere.api.java.functions.GroupReduceFunction.Combinable
	public static final class Reducer extends GroupReduceFunction<Tuple1<Long>,Long>{
		private static final long serialVersionUID = 1L;
		public static final String TOTAL_VERTICES = "num.vertices";
		private IntCounter counter = new IntCounter();
		
		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(TOTAL_VERTICES,
					counter);
		}
		@Override
		public void reduce(Iterator<Tuple1<Long>> values,
				Collector<Long> out) throws Exception {
			counter.add(1);
			Long srcKey = values.next().f0;
			out.collect(srcKey);
		}
		
		@Override
        public void combine(Iterator<Tuple1<Long>> values, Collector<Tuple1<Long>> out) {
            out.collect(values.next());
        }
		
	}
	
	
}

