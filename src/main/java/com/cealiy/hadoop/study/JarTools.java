package com.cealiy.hadoop.study;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class JarTools {

	public static File createJar(String path,String root) throws IOException {
		if (!new File(root).exists()) {
			return null;
		}
		Manifest manifest = new Manifest();
		manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
		File jarFile =new File(path);
		if(jarFile.exists()){
			return jarFile;
		}
		JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile), manifest);
		createJarInner(out, new File(root), "");
		out.flush();
		out.close();
		return jarFile;
	}

	
	private static void createJarInner(JarOutputStream out, File f, String base) throws IOException {
		if (f.isDirectory()) {
			File[] fl = f.listFiles();
			if (base.length() > 0) {
				base = base + "/";
			}
			for (int i = 0; i < fl.length; i++) {
				createJarInner(out, fl[i], base + fl[i].getName());
			}
		} else {
			out.putNextEntry(new JarEntry(base));
			FileInputStream in = new FileInputStream(f);
			byte[] buffer = new byte[1024];
			int n = in.read(buffer);
			while (n != -1) {
				out.write(buffer, 0, n);
				n = in.read(buffer);
			}
			in.close();
		}
	}

	
}
