package com.cealiy.hadoop.request.statistics;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class RequestWritable implements WritableComparable {

	private int count;

	private int totalExecuteTime;

	private int maxExecuteTime;

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getTotalExecuteTime() {
		return totalExecuteTime;
	}

	public void setTotalExecuteTime(int totalExecuteTime) {
		this.totalExecuteTime = totalExecuteTime;
	}

	public int getMaxExecuteTime() {
		return maxExecuteTime;
	}

	public void setMaxExecuteTime(int maxExecuteTime) {
		this.maxExecuteTime = maxExecuteTime;
	}

	public int getAverageExcuteTime() {
		return this.totalExecuteTime / this.count;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.count);
		out.writeInt(this.maxExecuteTime);
		out.writeInt(this.totalExecuteTime);
	}

	public void readFields(DataInput in) throws IOException {
		this.count = in.readInt();
		this.maxExecuteTime = in.readInt();
		this.totalExecuteTime = in.readInt();
	}

	public int compareTo(Object param) {
		return 0;
	}

	@Override
	public String toString() {
		return "{count:" + this.count + ",maxExecuteTime:" + this.maxExecuteTime + ",totalExecuteTime:"
				+ this.totalExecuteTime + "}";
	}

}
