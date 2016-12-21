package org.apache.hadoop.fs.http.client;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;


public class HttpFSFileSystemFactory {
	
	public static HttpFSFileSystem get(String uri,String user){
		HttpFSFileSystem fs=null;
		try {
			URI temp=new URI(uri);
			fs=new HttpFSFileSystem();
			fs.setHODOOP_HTTPFS_USER(user);
			Configuration conf=new Configuration();
			fs.initialize(temp,conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fs;
	}

}
