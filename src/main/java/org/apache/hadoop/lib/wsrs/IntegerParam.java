/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.lib.wsrs;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.lib.wsrs.Param;

@Private
public abstract class IntegerParam extends Param<Integer> {
	public IntegerParam(String name, Integer defaultValue) {
		super(name, defaultValue);
	}

	protected Integer parse(String str) throws Exception {
		return Integer.valueOf(Integer.parseInt(str));
	}

	protected String getDomain() {
		return "an integer";
	}
}