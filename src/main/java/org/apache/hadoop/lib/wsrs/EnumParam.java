/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.lib.wsrs;

import java.util.Arrays;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.lib.wsrs.Param;
import org.apache.hadoop.util.StringUtils;

@Private
public abstract class EnumParam<E extends Enum<E>> extends Param<E> {
	Class<E> klass;

	public EnumParam(String name, Class<E> e, E defaultValue) {
		super(name, defaultValue);
		this.klass = e;
	}

	protected E parse(String str) throws Exception {
		return Enum.valueOf(this.klass, StringUtils.toUpperCase(str));
	}

	protected String getDomain() {
		return StringUtils.join(",", Arrays.asList(this.klass.getEnumConstants()));
	}
}