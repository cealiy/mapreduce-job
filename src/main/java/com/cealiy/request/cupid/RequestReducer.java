package com.cealiy.request.cupid;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RequestReducer extends Reducer<Text,IntWritable,Text,RequestWritable>{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Reducer<Text,IntWritable,Text,RequestWritable>.Context context){
		try{
			int totalExecuteTime=0;
			int count=0;
			int maxExecuteTime=0;
			for(IntWritable value:values){
				if(value==null){
					continue;
				}
				totalExecuteTime=value.get()+totalExecuteTime;
				if(maxExecuteTime<value.get()){
					maxExecuteTime=value.get();
				}
				count++;
			}
			RequestWritable result=new RequestWritable();
			result.setCount(count);
			result.setMaxExecuteTime(maxExecuteTime);
			result.setTotalExecuteTime(totalExecuteTime);
			context.write(key,result);	
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
