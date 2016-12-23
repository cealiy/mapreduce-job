package com.cealiy.request.cupid;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RequestMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String log = value.toString();
		int flagStartIndex = log.indexOf("com.souche.cupid.interceptor.RequestInterceptor[run] - ");
		if (flagStartIndex == -1) {
			return;
		}
		int keyEndIndex = log.indexOf("--->");
		if (keyEndIndex == -1) {
			return;
		}
		int flagLength = "com.souche.cupid.interceptor.RequestInterceptor[run] - ".length();
		String uri = log.substring(flagStartIndex + flagLength, keyEndIndex);
		int costStartIndex = log.lastIndexOf(",");
		if (costStartIndex == -1) {
			return;
		}
		if (costStartIndex + 7 >= log.length() - 2) {
			return;
		}
		String cost = log.substring(costStartIndex + 7, log.length() - 2);
		int costms = 0;
		try {
			costms = Integer.parseInt(cost);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			context.write(new Text(uri), new IntWritable(costms));
		}
	}

}
