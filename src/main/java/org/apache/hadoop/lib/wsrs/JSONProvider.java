/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.lib.wsrs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.json.simple.JSONStreamAware;

@Provider
@Produces({ "application/json" })
@Private
public class JSONProvider implements MessageBodyWriter<JSONStreamAware> {
	private static final String ENTER = System.getProperty("line.separator");

	public boolean isWriteable(Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
		return JSONStreamAware.class.isAssignableFrom(aClass);
	}

	public long getSize(JSONStreamAware jsonStreamAware, Class<?> aClass, Type type, Annotation[] annotations,
			MediaType mediaType) {
		return -1L;
	}

	public void writeTo(JSONStreamAware jsonStreamAware, Class<?> aClass, Type type, Annotation[] annotations,
			MediaType mediaType, MultivaluedMap<String, Object> stringObjectMultivaluedMap, OutputStream outputStream)
					throws IOException, WebApplicationException {
		OutputStreamWriter writer = new OutputStreamWriter(outputStream, Charsets.UTF_8);
		jsonStreamAware.writeJSONString(writer);
		writer.write(ENTER);
		writer.flush();
	}
}