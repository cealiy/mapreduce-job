/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.fs.http.client;

import org.apache.hadoop.fs.http.client.HttpFSFileSystem;

public class HttpsFSFileSystem extends HttpFSFileSystem {
	public static final String SCHEME = "swebhdfs";

	public String getScheme() {
		return "swebhdfs";
	}
}