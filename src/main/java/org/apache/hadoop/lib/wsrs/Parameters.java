/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.lib.wsrs;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.lib.wsrs.Param;

@Private
public class Parameters {
	private Map<String, List<Param<?>>> params;

	public Parameters(Map<String, List<Param<?>>> params) {
		this.params = params;
	}

	public <V, T extends Param<V>> V get(String name, Class<T> klass) {
		List multiParams = (List) this.params.get(name);
		return (V) (multiParams != null && multiParams.size() > 0 ? ((Param) multiParams.get(0)).value() : null);
	}

	public <V, T extends Param<V>> List<V> getValues(String name, Class<T> klass) {
		List multiParams = (List) this.params.get(name);
		ArrayList values = Lists.newArrayList();
		if (multiParams != null) {
			Iterator arg4 = multiParams.iterator();

			while (arg4.hasNext()) {
				Param param = (Param) arg4.next();
				Object value = param.value();
				if (value != null) {
					values.add(value);
				}
			}
		}

		return values;
	}
}