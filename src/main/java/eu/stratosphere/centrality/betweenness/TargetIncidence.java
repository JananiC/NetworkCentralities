package eu.stratosphere.centrality.betweenness;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.accumulators.LongCounter;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.centrality.utils.CentralityUtil;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
/**
 * 
 * @author JANANI
 *
 */
public class TargetIncidence {

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err
					.println("Usage: LineRank <DOP> <edgeInputPath> <tar outputPath> <delimiter>");
			return;
		}
		
		final int dop = Integer.parseInt(args[0]);
		final String edgeInputPath = args[1];
		final String outputPath = args[2];
		String fieldDelimiter = CentralityUtil.TAB_DELIM;
        if(args.length>3){
        	fieldDelimiter = (args[3]);
        }
        
        char delim = CentralityUtil.checkDelim(fieldDelimiter);
		
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		env.setDegreeOfParallelism(dop);

		
		// mxv
		DataSet<Tuple3<Long, Long, Double>> tarIncMat = env
				.readCsvFile(edgeInputPath).fieldDelimiter(delim)
				.types(Long.class, Long.class)
				.map(new TargetIncMatrix()).name("T(G)");
		
	
		tarIncMat.writeAsCsv(outputPath, CentralityUtil.NEWLINE, CentralityUtil.TAB_DELIM, WriteMode.OVERWRITE);
		JobExecutionResult job = env.execute();
		System.out.println("RunTime-->"+ ((job.getNetRuntime()/1000))+"sec");
	}

	
	/**
	 * Reads input edge file <srcId,tarId,weight>. Generates edgeId using accumulators and emits <edgeId, tarId, weight>
	 */	
	public static final class TargetIncMatrix extends
			MapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Double>> {
		private static final long serialVersionUID = 1L;
		public static final String ACCUM_NUM_LINES = "accumulator.num-lines";
		private LongCounter num_vertices = new LongCounter();
		
		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(ACCUM_NUM_LINES,
					this.num_vertices);
		}
		
		@Override
		public Tuple3<Long, Long, Double> map(Tuple2<Long, Long> value)
				throws Exception {
			num_vertices.add(1L);
			Tuple3<Long, Long, Double> tarInc = new Tuple3<Long, Long, Double>();
			tarInc.f0 = num_vertices.getLocalValue().longValue();
			tarInc.f1 = value.f1;
			tarInc.f2 = 1.0;
			//System.out.println("Tar-->"+tarInc.f0+" "+tarInc.f1+" "+tarInc.f2);
			return tarInc;
		}
	}

}
