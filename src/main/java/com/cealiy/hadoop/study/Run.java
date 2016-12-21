package com.cealiy.hadoop.study;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.http.client.HttpFSFileSystem;
import org.apache.hadoop.fs.http.client.HttpFSFileSystemFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.cealiy.hadoop.request.statistics.RequestMapper;
import com.cealiy.hadoop.request.statistics.RequestReducer;
import com.cealiy.hadoop.request.statistics.RequestStatistics;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

public class Run {
	
	public static void test() throws JSchException, IOException, SftpException{
		SSHClient client=new SSHClient("182.254.245.178","root","cqy19921230");
		client.put("/Users/chenqingyuan/hadoop/tomcat.log","/root/tomcat.log");
	}

	public static void main(String[] args) {
		
		try{
			System.out.println(Run.class.getResource(""));
			JarTools.createJar("/Users/chenqingyuan/hadoop/requset-statics.jar", RequestStatistics.class.getResource("").getFile());
			
//			Configuration c = new Configuration(false);
//     		c.set("fs.file.impl", org.apache.hadoop.fs.http.client.HttpFSFileSystem.class.getName());
//     		//c.set("fs.defaultFS","webhdfs://master:14000");
//     		URI uri=new URI("webhdfs://master:14000");
     		HttpFSFileSystem fs=HttpFSFileSystemFactory.get("webhdfs://master:14000","root");
//			fs.copyFromLocalFile(new Path("/Users/chenqingyuan/hadoop/tomcat.log"),new Path("webhdfs://master:14000/input/tomcat.log"));
			FSDataOutputStream out1=fs.create(new Path("webhdfs://master:14000/input/test.txt"));
			out1.writeBytes("just have fun");
			out1.flush();
			out1.close();
			FSDataInputStream in1=fs.open(new Path("webhdfs://master:14000/input/test.txt"));
			System.out.println(in1.readLine());
			boolean f=fs.delete(new Path("webhdfs://master:14000/input/test.txt"),false);
			System.out.println(f);
			//MRAppmaster mr=new MRAppmaster(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, clock, appSubmitTime);
			Configuration conf = new Configuration(false);
//			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
			String origin="/Users/chenqingyuan/hadoop/tomcat.log";
//			String in = "hdfs://localhost:9000/input/tomcat.log";
//			String out = "hdfs://localhost:9000/output";
			conf.set("fs.defaultFS","hdfs://master:9000");
//			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//			conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//			String origin="/Users/chenqingyuan/hadoop/tomcat.log";
			String in = "hdfs://master:9000/input/test.txt";
			String out = "hdfs://master:9000/output";
	        conf.set("hadoop.job.user","root");
	        conf.set("yarn.resourcemanager.resource-tracker.address", "master:8031");  
	        conf.set("yarn.resourcemanager.address", "master:8032");  
	        conf.set("yarn.resourcemanager.scheduler.address", "master:8030");  
	        conf.set("yarn.resourcemanager.admin.address", "master:8033");  
	        conf.set("yarn.application.classpath", "$HADOOP_CONF_DIR,"  
	            +"$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"  
	            +"$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"  
	            +"$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,"  
	            +"$YARN_HOME/*,$YARN_HOME/lib/*,"  
	            +"$HBASE_HOME/*,$HBASE_HOME/lib/*,$HBASE_HOME/conf/*"); 
	        conf.set("mapreduce.jobtracker.address","master:50030");
	        conf.set("mapreduce.tasktracker.http.address","master:50060");
	        conf.set("mapreduce.jobhistory.address", "master:10020");  
	        conf.set("mapreduce.jobhistory.webapp.address", "master:19888");  
	        conf.set("mapred.child.java.opts", "-Xmx1024m");  

		//Hdfs.copyFile(origin,in,conf);
			Job job = Job.getInstance(conf, "statistics request");
			JobConf jobConf=new JobConf();
			job.setMapperClass(RequestMapper.class);
			job.setReducerClass(RequestReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(in));
			FileOutputFormat.setOutputPath(job, new Path(out));
			job.waitForCompletion(true);
			System.out.println("Finished");
			System.out.println(Hdfs.readFile(out+"/part-r-00000",conf));
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}

}
