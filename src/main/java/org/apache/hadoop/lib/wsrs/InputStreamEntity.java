/*** Eclipse Class Decompiler plugin, copyright (c) 2016 Chen Chao (cnfree2000@hotmail.com) ***/
package org.apache.hadoop.lib.wsrs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.ws.rs.core.StreamingOutput;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.IOUtils;

@Private
public class InputStreamEntity implements StreamingOutput {
	private InputStream is;
	private long offset;
	private long len;

	public InputStreamEntity(InputStream is, long offset, long len) {
		this.is = is;
		this.offset = offset;
		this.len = len;
	}

	public InputStreamEntity(InputStream is) {
		this(is, 0L, -1L);
	}

	public void write(OutputStream os) throws IOException {
		IOUtils.skipFully(this.is, this.offset);
		if (this.len == -1L) {
			IOUtils.copyBytes(this.is, os, 4096, true);
		} else {
			IOUtils.copyBytes(this.is, os, this.len, true);
		}

	}
}