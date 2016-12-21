package com.cealiy.request.cupid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RequestStatistics {
	
	public static void statistics(String in,String out) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "statistics-cupid-request");
	    job.setJarByClass(RequestStatistics.class);
		job.setMapperClass(RequestMapper.class);
		job.setReducerClass(RequestReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
