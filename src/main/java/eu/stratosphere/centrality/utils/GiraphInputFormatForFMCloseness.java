package eu.stratosphere.centrality.utils;

import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.util.Collector;
/**
 * 
 * @author JANANI
 *
 */
public class GiraphInputFormatForFMCloseness {

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
		
		DataSet<Tuple2<Long, Long>> first = env.readCsvFile(inputfilepath).fieldDelimiter(delim)
				.types(Long.class,Long.class);
		
		DataSet<Tuple2<Long, String>> result =  first.groupBy(0).reduceGroup(new Reducer());
		
		result.writeAsCsv(outputfilepath,  "\n", CentralityUtil.TAB_DELIM, WriteMode.OVERWRITE);
		
		env.execute();
		}

	
	public static final class Reducer extends GroupReduceFunction<Tuple2<Long,Long>,Tuple2<Long,String>>{
		private static final long serialVersionUID = 1L;
		long srcKey;
		@Override
		public void reduce(Iterator<Tuple2<Long, Long>> values,
				Collector<Tuple2<Long, String>> out) throws Exception {
			boolean flag = true;
			String tarKeys = "";
			while(values.hasNext()) {
				Tuple2<Long,Long> tuple = values.next();
				if(flag){
				 srcKey= tuple.f0.longValue();
				 tarKeys = tuple.f1.toString();
				 flag = false;
				}else{
				tarKeys = tarKeys+","+tuple.f1.toString();
				}
			}
			Tuple2<Long,String> srcTarList = new Tuple2<Long,String>();
			srcTarList.f0 = srcKey;
			srcTarList.f1 = tarKeys;
			out.collect(srcTarList);
		}
		
	}
	

}
