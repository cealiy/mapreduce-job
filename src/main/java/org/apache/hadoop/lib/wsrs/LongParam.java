/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.lib.wsrs;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.lib.wsrs.Param;

@Private
public abstract class LongParam extends Param<Long> {
	public LongParam(String name, Long defaultValue) {
		super(name, defaultValue);
	}

	protected Long parse(String str) throws Exception {
		return Long.valueOf(Long.parseLong(str));
	}

	protected String getDomain() {
		return "a long";
	}
}