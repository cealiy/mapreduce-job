/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.lib.wsrs;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.lib.wsrs.Param;

@Private
public abstract class ShortParam extends Param<Short> {
	private int radix;

	public ShortParam(String name, Short defaultValue, int radix) {
		super(name, defaultValue);
		this.radix = radix;
	}

	public ShortParam(String name, Short defaultValue) {
		this(name, defaultValue, 10);
	}

	protected Short parse(String str) throws Exception {
		return Short.valueOf(Short.parseShort(str, this.radix));
	}

	protected String getDomain() {
		return "a short";
	}
}