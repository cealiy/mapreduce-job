package com.cealiy.hadoop.study;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class Hdfs 
{
    
    
    public static String readFile(String path,Configuration conf) throws Exception{
    	try{
    		FileSystem fs=FileSystem.get(URI.create(path),conf);
        	InputStream in=fs.open(new Path(path));
        	StringBuilder sb=new StringBuilder();
        	byte[] bytes=new byte[4096];
        	while((in.read(bytes,0,4096))!=-1){
        		String s=new String(bytes,"UTF-8");
        		sb.append(s);
        	}
        	IOUtils.closeStream(in);
        	return sb.toString();
    		
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    	return null;
    	
    }
    
    
    public static void copyFile(String from ,String to,Configuration conf) throws Exception{
    	try{
    		InputStream in=new BufferedInputStream(new FileInputStream(from));
        	FileSystem fs=FileSystem.get(URI.create(to),conf);
        	OutputStream out=fs.create(new Path(to),new Progressable() {
    			public void progress() {
    				System.out.println("...");
    			}
    		});
        	IOUtils.copyBytes(in, out,4096);
        	IOUtils.closeStream(in);
        	IOUtils.closeStream(out);
    	}catch(Exception e){
    		e.printStackTrace();
    	}	
    }
    
    public static void copyFromLocalFile() throws IOException, URISyntaxException{
    	FileSystem fs=FileSystem.get(new URI("hdfs://master:9000"),new Configuration());
    	fs.copyFromLocalFile(new Path("/Users/chenqingyuan/hadoop/tomcat.log"),new Path("/input/tomcat.log"));
    	
    }
    
}
