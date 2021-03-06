package eu.stratosphere.centrality.betweenness;

import java.util.Iterator;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.accumulators.IntCounter;
import eu.stratosphere.api.common.aggregators.ConvergenceCriterion;
import eu.stratosphere.api.common.aggregators.DoubleSumAggregator;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.centrality.utils.CentralityUtil;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.util.Collector;
/**
 * 
 * @author JANANI
 *
 */
@SuppressWarnings("serial")
public class UnweightedLineRank {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err
					.println("Usage: LineRank <DOP> <edgeInputPath> <outputPath> <numIterations> <numOfEdges> <delimiter>");
			return;
		}
		
		final int dop = Integer.parseInt(args[0]);
		
		final String srcIncidencePath = args[1];
		final String targetInputPath = args[2];
		final String outputPath = args[3];
		final int maxIterations = Integer.parseInt(args[4]);
		final double numEdges = (args.length > 5 ? (Integer.parseInt(args[5])) : 1);
		Double c = 0.85;
		String fieldDelimiter = CentralityUtil.TAB_DELIM;
        if(args.length>6){
        	fieldDelimiter = (args[6]);
        }
        char delim = CentralityUtil.checkDelim(fieldDelimiter);
		
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		env.setDegreeOfParallelism(dop);

		/****************************************************
		  Building incidence matrices S(G) and T(G) 
		 ****************************************************/
		// mxv
		DataSet<Tuple2<Long, Long>> srcIncMat = env
				.readCsvFile(srcIncidencePath).fieldDelimiter(delim)
				.types(Long.class, Long.class);
		// mxv
		DataSet<Tuple2<Long, Long>> tarIncMat = env
				.readCsvFile(targetInputPath).fieldDelimiter(delim)
				.types(Long.class, Long.class);
		
		/****************************************************
		 *  Computing normalization factors 
		 ****************************************************/
		// d1 <- S(G)T*1 which results in d1 of dimensions vxm X mxv  => vx1
				DataSet<Tuple2<Long, Double>> d1 = srcIncMat.groupBy(1).reduceGroup(
						new MatrixToVector()).name("D1");
				// d2 <- T(G)d1; which results in d2 of dimensions mxv X vx1 => mx1
				DataSet<Tuple2<Long, Double>> d2 = d1.join(tarIncMat).where(0)
						.equalTo(1).with(new MatrixVectorMul()).name("D2");

				// d <- 1./d2 an element-wise divide operation, so no dimension change=> mx1
				DataSet<Tuple2<Long, Double>> d = d2.map(new MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<Long, Double> map(Tuple2<Long, Double> value)
							throws Exception {
						Tuple2<Long, Double> elementwiseInverse = new Tuple2<Long, Double>();
						elementwiseInverse.f0 = value.f0;
						elementwiseInverse.f1 = 1 / value.f1;
						//System.out.println("d-->"+elementwiseInverse.f0+"  "+elementwiseInverse.f1);
						return elementwiseInverse;
					}
				}).name("D");
				//Initialize random vector with mx1
				DataSet<Tuple2<Long, Double>> edgeScores = d.map(new InitializeRandomVector(numEdges)).name("V");
		
		/********************************************************
		  Power Method for computing the stationary probabilities
		  of edges using Bulk Iteration 
		********************************************************/
		
		IterativeDataSet<Tuple2<Long,Double>> iteration = edgeScores.iterate(maxIterations)
			.registerAggregationConvergenceCriterion(L1_NormDiff.AGGREGATOR_NAME, DoubleSumAggregator.class, L1_NormConvergence.class)
				.name("EdgeScoreVector_BulkIteration");
		
		DataSet<Tuple2<Long, Double>> new_edgeScores = iteration
					.join(d).where(0).equalTo(0).with(new V1_HadamardProduct()).name("V1") //Hadamard product of v1 <- d * v 
					.join(srcIncMat).where(0).equalTo(0).with(new V2_SrcIncWithV1()).name("V2") //S(G) i.e. mxv becomes => vxm  and then vxm X mx1 => vx1
					.groupBy(0).aggregate(Aggregations.SUM, 1) //Sum followed by product in matrix vector multiplication would result vx1
					//.map(new PrintMapper("v2"))
					.join(tarIncMat).where(0).equalTo(1).with(new V3_TarIncWithV2(c,numEdges)).name("V3")
					//.map(new DampingMapper(c, numEdges)) // mxv X vx1   => mx1
					//.map(new PrintMapper("v3"))
					.join(iteration).where(0).equalTo(0).with(new L1_NormDiff()).name("L1_NORM"); 
		DataSet<Tuple2<Long, Double>> convergedVector = iteration.closeWith(new_edgeScores);
		
		
	
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
		System.out.println("Total number of iterations in UnweightedLineRank-->"+((job.getIntCounterResult(V2_SrcIncWithV1.ACCUM_LOCAL_ITERATIONS)/dop)-1));
		System.out.println("RunTime-->"+ ((job.getNetRuntime()))+"sec");
	}

	
	
	
	/**
	 * Convergence criterion to check the sum of the differences of edge scores is less than a threshold at the end of each iteration
	 */
	public static final class L1_NormConvergence implements ConvergenceCriterion<DoubleValue>{

		private static final double EPSILON = 0.00001;

		public boolean isConverged(int iteration, DoubleValue value) {
			double diff = value.getValue();
			//System.out.println("inside check");
			return diff < EPSILON;
		}
	}
	/**
	 * Joins the current edge score vector v with previous iteration's vector v to find the differences
	 */
	public static final class L1_NormDiff extends JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {
		public static final String AGGREGATOR_NAME = "linerank.aggregator";
		private DoubleSumAggregator  agg;
		
		public void open(Configuration parameters) {
			          agg = getIterationRuntimeContext().getIterationAggregator(AGGREGATOR_NAME);
		}
		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> current,
				Tuple2<Long, Double> prev) throws Exception {
			//System.out.println(current.f0+"---prev-->"+prev.f1+"   -   "+current.f1+"  at step-->"+getIterationRuntimeContext().getSuperstepNumber());
			agg.aggregate(Math.abs(prev.f1 - current.f1));
			return current;
		}
	}
	/**
	 * An intermediate join operation in the iteration between V1 vector and S(G) on vector index of V1 and edgeId of S(G) 
	 */
	@ConstantFieldsFirst("1 -> 1")
	@ConstantFieldsSecond("1 -> 0")
	public static final class V2_SrcIncWithV1
			extends
			JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Long>, Tuple2<Long, Double>> {
		public static final String ACCUM_LOCAL_ITERATIONS = "accum.local.iterations";
		private IntCounter localIterations = new IntCounter();
		
		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(ACCUM_LOCAL_ITERATIONS,
					localIterations);
			localIterations.add(1);
		}
		// v2 <- S(G)T v1;

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> iteration_v1,
				Tuple2<Long, Long> srcIncMat) throws Exception {
			Tuple2<Long, Double> result = new Tuple2<Long, Double>();
			result.f0 = srcIncMat.f1;
			result.f1 = (iteration_v1.f1);
			//System.out.println("-srcinc->"+result.f0+"---->"+iteration_v1.f1+"  *  "+srcIncMat.f2);
			return result;
		}
	}
	/**
	 * A join operation to get the product of two vectors (d and v)
	 */
	@ConstantFieldsFirst("0->0")
	public static final class V1_HadamardProduct
			extends
			JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {
	
		// v1 <- dv  Hadamard product
		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> v,
				Tuple2<Long, Double> d) throws Exception {
			Tuple2<Long, Double> result = new Tuple2<Long, Double>();
			result.f0 = v.f0;
			result.f1 = (d.f1) * v.f1;
			//System.err.println("Iteration "+getIterationRuntimeContext().getSuperstepNumber());
			//System.out.println("prod -->"+d.f1+"   *   "+v.f1);
			return result;
		}
	}
	/**
	 * A map function to randomly initialize edge score vector. An initial value to all the edges in the graph
	 */
	@ConstantFields("0")
	public static final class InitializeRandomVector extends
			MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
		private final double fracNumEdges;
		// random initial vector of size m
		public InitializeRandomVector(double num){
			fracNumEdges = 1/num;
		}
		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value)
				throws Exception {
			Tuple2<Long, Double> v = new Tuple2<Long, Double>();
			v.f0 = value.f0;
			v.f1 = fracNumEdges;
		//	System.out.println("random vector -->"+v.f0+"    "+v.f1);
			return v;
		}
	}
	/**
	 * Reused Join function for getting the product in the matrix vector multiplication
	 * (the sum followed by this product is achieved by using group by aggregate)
	 */
	@ConstantFieldsFirst("1->1")
	@ConstantFieldsSecond("0->0")
	public static final class MatrixVectorMul
			extends
			JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Long>, Tuple2<Long, Double>> {
		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> first,
				Tuple2<Long, Long> second) throws Exception {
			Tuple2<Long, Double> result = new Tuple2<Long, Double>();
			
				result.f0 = second.f0;
				result.f1 = first.f1;
			
			return result;
		}
	}
	
	
	@ConstantFieldsSecond("0 -> 0")
	public static final class V3_TarIncWithV2
	extends
	JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Long>, Tuple2<Long, Double>> {
		private final Double c;
		private final double randomJump;
		public V3_TarIncWithV2(Double c, Double numEdges) {
			this.c = c;
			this.randomJump =  (1 - c) /numEdges;
		}

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> first,
				Tuple2<Long, Long> second) throws Exception {
			Tuple2<Long, Double> result = new Tuple2<Long, Double>();
			
			result.f0 = second.f0;
			result.f1 = (first.f1) * c + randomJump;
			return result;
					
		}
}
	
	public static final class DampingMapper extends MapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>> {

		private final double dampening;
		private final double randomJump;
		
		public DampingMapper(double dampening, double numEdges) {
			this.dampening = dampening;
			this.randomJump = (1 - dampening) / numEdges;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
			value.f1 = (value.f1 * dampening) + randomJump;
			return value;
		}
	}
	
/*	*//**
	 * A reduce operation used as a part of normalization
	 *//*
	public static final class MatrixToVector
			extends
			GroupReduceFunction<Tuple3<Long, Long, Double>, Tuple2<Long, Double>> {
		@Override
		public void reduce(Iterator<Tuple3<Long, Long, Double>> values,
				Collector<Tuple2<Long, Double>> out) throws Exception {
			Tuple2<Long, Double> toVector = new Tuple2<Long, Double>();
			Double sum = 0.0;
			boolean flag = false;
			Long key = null;
			while (values.hasNext()) {
				Tuple3<Long, Long, Double> sameRowValues = values.next();
				if (!flag) {
					key = sameRowValues.f1;
					flag = true;
				}
				sum = sum + sameRowValues.f2;
			}
			
			toVector.f0 = key;
			toVector.f1 = sum;
		//	System.out.println("d1 -->"+toVector.f0+"  "+toVector.f1);
			out.collect(toVector);
		}
		
	}*/
	
	/**
	 * A reduce operation used as a part of normalization
	 */
	public static final class MatrixToVector
			extends
			GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
		@Override
		public void reduce(Iterator<Tuple2<Long, Long>> values,
				Collector<Tuple2<Long, Double>> out) throws Exception {
			Tuple2<Long, Double> toVector = new Tuple2<Long, Double>();
			Double sum = 0.0;
			boolean flag = false;
			Long key = null;
			while (values.hasNext()) {
				Tuple2<Long, Long> sameRowValues = values.next();
				if (!flag) {
					key = sameRowValues.f1;
					flag = true;
				}
				sum = sum + 1;
			}
			
			toVector.f0 = key;
			toVector.f1 = sum;
		//	System.out.println("d1 -->"+toVector.f0+"  "+toVector.f1);
			out.collect(toVector);
		}
		
	}
	
	/**
	 * A join function for to compute the partial (product part in matrix vector multiplication) aggregation
	 */
	@ConstantFieldsFirst("1 -> 0")
	@ConstantFieldsSecond("1 -> 1")
	public static final class AddSrcWithTar
	extends
	JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Double>, Tuple2<Long, Double>> {
		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Long> matrix,
				Tuple2<Long, Double> vector) throws Exception {
			Tuple2<Long, Double> transposed = new Tuple2<Long, Double>();
			transposed.f0 = matrix.f1;
			transposed.f1 = vector.f1;
			return transposed;
		}
	}
	/**
	 * A final join to compute full aggregation
	 */
	@ConstantFieldsFirst("0")
	public static final class EdgeScoreAggregation
			extends
			JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

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
