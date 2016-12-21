/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.lib.wsrs;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.util.HttpExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
public class ExceptionProvider implements ExceptionMapper<Throwable> {
	private static Logger LOG = LoggerFactory.getLogger(ExceptionProvider.class);
	private static final String ENTER = System.getProperty("line.separator");

	protected Response createResponse(Status status, Throwable throwable) {
		return HttpExceptionUtils.createJerseyExceptionResponse(status, throwable);
	}

	protected String getOneLineMessage(Throwable throwable) {
		String message = throwable.getMessage();
		if (message != null) {
			int i = message.indexOf(ENTER);
			if (i > -1) {
				message = message.substring(0, i);
			}
		}

		return message;
	}

	protected void log(Status status, Throwable throwable) {
		LOG.debug("{}", throwable.getMessage(), throwable);
	}

	public Response toResponse(Throwable throwable) {
		return this.createResponse(Status.BAD_REQUEST, throwable);
	}
}