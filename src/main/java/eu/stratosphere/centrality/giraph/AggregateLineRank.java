package eu.stratosphere.centrality.giraph;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.centrality.utils.CentralityUtil;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
/**
 * 
 * @author JANANI
 *
 */
public class AggregateLineRank {
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err
					.println("Usage: LineRank <DOP> <srcInputPath> <tarInputPath> <edgescorevector> <outputPath>");
			return;
		}
		
		final int dop = Integer.parseInt(args[0]);
		final String srcIncidencePath = args[1];
		final String targetInputPath = args[2];
		final String vectorPath = args[3];
		final String outputPath = args[4];
        
        char delim = '\t';
		
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		env.setDegreeOfParallelism(dop);

		/****************************************************
		  Building incidence matrices S(G) and T(G) 
		 ****************************************************/
		// mxv
		DataSet<Tuple3<Long, Long, Double>> srcIncMat = env
				.readCsvFile(srcIncidencePath).fieldDelimiter(delim)
				.types(Long.class, Long.class,Double.class);
		// mxv
		DataSet<Tuple3<Long, Long, Double>> tarIncMat = env
				.readCsvFile(targetInputPath).fieldDelimiter(delim)
				.types(Long.class, Long.class,Double.class);
		
		DataSet<Tuple2<Long, Double>> convergedVector = env
				.readCsvFile(vectorPath).fieldDelimiter(delim)
				.types(Long.class, Double.class);
		
		
		
		/******************************************************** 
		  Aggregating edge scores for each vertex to 
		  get betweenness score
		********************************************************/
		// (S(G) + T(G))^T * V => S(G)^T *V + T(G)^T *V

		DataSet<Tuple2<Long, Double>> partialAggregation_1 = srcIncMat.join(convergedVector)
				.where(0).equalTo(0).with(new AddSrcWithTar()).name("Par1")
				.groupBy(0).aggregate(Aggregations.SUM, 1);
		DataSet<Tuple2<Long, Double>> partialAggregation_2 = tarIncMat.join(convergedVector)
				.where(0).equalTo(0).with(new AddSrcWithTar()).name("Part2")
				.groupBy(0).aggregate(Aggregations.SUM, 1);
		DataSet<Tuple2<Long, Double>> lineRank = partialAggregation_1.join(partialAggregation_2)
				.where(0).equalTo(0).with(new EdgeScoreAggregation()).name("EdgeScore_Agg");

		
		
		lineRank.writeAsCsv(outputPath, CentralityUtil.NEWLINE, CentralityUtil.TAB_DELIM, WriteMode.OVERWRITE).name("Writing Results");
		JobExecutionResult job = env.execute();
		System.out.println("RunTime Agg-->"+ ((job.getNetRuntime()))+"sec");
	}
	
	/**
	 * A join function for to compute the partial (product part in matrix vector multiplication) aggregation
	 */
	public static final class AddSrcWithTar
	extends
	JoinFunction<Tuple3<Long, Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<Long, Double> join(Tuple3<Long, Long, Double> matrix,
				Tuple2<Long, Double> vector) throws Exception {
			Tuple2<Long, Double> transposed = new Tuple2<Long, Double>();
			transposed.f0 = matrix.f1;
			transposed.f1 = matrix.f2 * vector.f1;
			return transposed;
		}
	}
	/**
	 * A final join to compute full aggregation
	 */
	public static final class EdgeScoreAggregation
			extends
			JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> first,
				Tuple2<Long, Double> second) throws Exception {
			Tuple2<Long, Double> res = new Tuple2<Long, Double>();
			res.f0 = first.f0;
			res.f1 = first.f1 + second.f1;
			//System.out.println("Line Rank of " + res.f0 + " -> " + res.f1);
			return res;
		}

	}
}
