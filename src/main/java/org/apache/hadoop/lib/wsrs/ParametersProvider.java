/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.lib.wsrs;

import com.google.common.collect.Lists;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;
import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.lib.wsrs.Param;
import org.apache.hadoop.lib.wsrs.Parameters;
import org.apache.hadoop.util.StringUtils;

@Private
public class ParametersProvider extends AbstractHttpContextInjectable<Parameters>
		implements InjectableProvider<Context, Type> {
	private String driverParam;
	private Class<? extends Enum> enumClass;
	private Map<Enum, Class<Param<?>>[]> paramsDef;

	public ParametersProvider(String driverParam, Class<? extends Enum> enumClass,
			Map<Enum, Class<Param<?>>[]> paramsDef) {
		this.driverParam = driverParam;
		this.enumClass = enumClass;
		this.paramsDef = paramsDef;
	}

	public Parameters getValue(HttpContext httpContext) {
		HashMap map = new HashMap();
		MultivaluedMap queryString = httpContext.getRequest().getQueryParameters();
		String str = (String) ((MultivaluedMap) queryString).getFirst(this.driverParam);
		if (str == null) {
			throw new IllegalArgumentException(
					MessageFormat.format("Missing Operation parameter [{0}]", new Object[] { this.driverParam }));
		} else {
			Enum op;
			try {
				op = Enum.valueOf(this.enumClass, StringUtils.toUpperCase(str));
			} catch (IllegalArgumentException arg16) {
				throw new IllegalArgumentException(
						MessageFormat.format("Invalid Operation [{0}]", new Object[] { str }));
			}

			if (!this.paramsDef.containsKey(op)) {
				throw new IllegalArgumentException(
						MessageFormat.format("Unsupported Operation [{0}]", new Object[] { op }));
			} else {
				Class[] ex = (Class[]) this.paramsDef.get(op);
				int arg6 = ex.length;

				for (int arg7 = 0; arg7 < arg6; ++arg7) {
					Class paramClass = ex[arg7];
					Param param = this.newParam(paramClass);
					ArrayList paramList = Lists.newArrayList();
					List ps = (List) queryString.get(param.getName());
					if (ps != null) {
						for (Iterator arg12 = ps.iterator(); arg12.hasNext(); param = this.newParam(paramClass)) {
							String p = (String) arg12.next();

							try {
								param.parseParam(p);
							} catch (Exception arg15) {
								throw new IllegalArgumentException(arg15.toString(), arg15);
							}

							paramList.add(param);
						}
					} else {
						paramList.add(param);
					}

					map.put(param.getName(), paramList);
				}

				return new Parameters(map);
			}
		}
	}

	private Param<?> newParam(Class<Param<?>> paramClass) {
		try {
			return (Param) paramClass.newInstance();
		} catch (Exception arg2) {
			throw new UnsupportedOperationException(MessageFormat.format(
					"Param class [{0}] does not have default constructor", new Object[] { paramClass.getName() }));
		}
	}

	public ComponentScope getScope() {
		return ComponentScope.PerRequest;
	}

	public Injectable getInjectable(ComponentContext componentContext, Context context, Type type) {
		return type.equals(Parameters.class) ? this : null;
	}
}