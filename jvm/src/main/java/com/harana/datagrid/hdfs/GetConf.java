package com.harana.datagrid.hdfs;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;

public class GetConf {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("Needs at least one argument");
		}
		
		String key = args[0];
		Configuration conf = new Configuration();
		if (key.equalsIgnoreCase("namenode")) {
			String defaultFS = conf.get("fs.defaultFS");
			URI uri = new URI(defaultFS);
			System.out.println(uri.getHost());
		} else {
			String value = conf.get(key);
			System.out.println(value);
		}
	}

}
