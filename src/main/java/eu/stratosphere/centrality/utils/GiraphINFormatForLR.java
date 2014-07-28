package eu.stratosphere.centrality.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.util.Collector;
/**
 * 
 * @author JANANI
 *
 */
public class GiraphINFormatForLR {
	private static final String SPACE = " ";
	private static final String COLON = ":";
	private static final String TRUE =  "true";
	private static final String FALSE = "false";
	public static void main(String[] args) throws Exception {

		if (args.length < 3) {
			System.err.println("Usage:[Degree of parallelism],[edge input-path],[out-put],[delimiter]");
			return;
		}
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0])
				: 1);
		final String inputfilepath = (args.length > 1 ? args[1] : "");
		final String outputfilepath = (args.length > 2 ? args[2] : "");
	     String fieldDelimiter = CentralityUtil.TAB_DELIM;
	        if(args.length>3){
	        	fieldDelimiter = (args[3]);
	        }
	    char delim = CentralityUtil.checkDelim(fieldDelimiter);
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		env.setDegreeOfParallelism(numSubTasks);
		
		DataSet<Tuple2<Long, Long>> readFile = env.readCsvFile(inputfilepath).fieldDelimiter(delim)
				.types(Long.class,Long.class);
		
		DataSet<Tuple2<Long, MyCollection>> first =  readFile.groupBy(0).reduceGroup(new Reducer());
		DataSet<Tuple2<Long, MyCollection>> second =  readFile.groupBy(1).reduceGroup(new Reducer1());
		
		DataSet<Tuple2<Long,String>> sink = first.join(second).where(0).equalTo(0).with(new Formating());
		
		sink.writeAsCsv(outputfilepath,  "\n", SPACE, WriteMode.OVERWRITE);
		
		env.execute();
		}


	public static final class Reducer extends GroupReduceFunction<Tuple2<Long,Long>,Tuple2<Long,MyCollection>>{
		private static final long serialVersionUID = 1L;
		long srcKey;
		@Override
		public void reduce(Iterator<Tuple2<Long, Long>> values,
				Collector<Tuple2<Long, MyCollection>> out) throws Exception {
			boolean flag = true;
			List<Long> list = new ArrayList<Long>();
			
			while(values.hasNext()) {
				Tuple2<Long,Long> tuple = values.next();
				if(flag){
				 srcKey= tuple.f0;
				 flag = false;
				}
				list.add(tuple.f1);
			}
			Collections.sort(list);
			MyCollection myColl = new MyCollection(list); 
			//System.out.println(srcKey +"---->"+list);
			Tuple2<Long,MyCollection> srcTarList = new Tuple2<Long,MyCollection>();
			srcTarList.f0 = srcKey;
			srcTarList.f1 = myColl;
			out.collect(srcTarList);
		}
		
	}
	
	public static final class Reducer1 extends GroupReduceFunction<Tuple2<Long,Long>,Tuple2<Long,MyCollection>>{
		private static final long serialVersionUID = 1L;
		long tarKey;
		@Override
		public void reduce(Iterator<Tuple2<Long, Long>> values,
				Collector<Tuple2<Long, MyCollection>> out) throws Exception {
			boolean flag = true;
			List<Long> list = new ArrayList<Long>();
			
			while(values.hasNext()) {
				Tuple2<Long,Long> tuple = values.next();
				if(flag){
				 tarKey= tuple.f1;
				 flag = false;
				}
				list.add(tuple.f0);
			}
			Collections.sort(list);
			MyCollection myColl = new MyCollection(list); 
			//System.out.println(tarKey +"---->"+list);
			Tuple2<Long,MyCollection> srcTarList = new Tuple2<Long,MyCollection>();
			srcTarList.f0 = tarKey;
			srcTarList.f1 = myColl; 
			
			out.collect(srcTarList);
		}
		
	}
	
	
	public static final class Formating extends
	JoinFunction<Tuple2<Long, MyCollection>, Tuple2<Long, MyCollection>, Tuple2<Long, String>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<Long, String> join(
				Tuple2<Long, MyCollection> first,
				Tuple2<Long, MyCollection> second) throws Exception {
			List<Long> tarList = first.f1.getList();
			List<Long> srcList = second.f1.getList();
			String finalStr = "";
			List<Long> duplist = new ArrayList<Long>();
			
			for(Long tar : tarList){
				if(srcList != null){
				if(srcList.contains(tar)){
					finalStr = finalStr+tar+COLON+TRUE+COLON+TRUE+SPACE;
					duplist.add(tar);
				}else{
					finalStr = finalStr+tar+COLON+FALSE+COLON+TRUE+SPACE;
				}
				}
			}
			
			for(Long src : srcList){
				if(!duplist.contains(src)){
					if(tarList != null){
						if(tarList.contains(src)){
							finalStr = finalStr+src+COLON+TRUE+COLON+TRUE+SPACE;
						}else {
							finalStr = finalStr+src+COLON+TRUE+COLON+FALSE+SPACE;
						}
					}
				}
			
			}
			//System.out.println(finalStr);
			//System.out.println();
			Tuple2<Long,String> result = new Tuple2<Long, String>();
			result.f0 = first.f0;
			result.f1 = finalStr;
			return result;
		}
	
	}
	
	public static class MyCollection {
		private List<Long> list;
		public MyCollection(){
			
		}
		public MyCollection(List<Long> list){
			this.list = list;
		}

		public List<Long> getList() {
			return list;
		}

		
	}

}
