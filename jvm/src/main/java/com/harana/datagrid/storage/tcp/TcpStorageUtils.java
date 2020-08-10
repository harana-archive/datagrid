package com.harana.datagrid.storage.tcp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;

public class TcpStorageUtils {
	private static final Logger logger = LogManager.getLogger();
	public static void printBuffer(String name, ByteBuffer buffer){
		String state = buffer.toString();
		String data = "";
		for (int i = 0; i < buffer.remaining(); i++){
			data += buffer.get(buffer.position() + i) + ",";
		}
		logger.info("buffer " + name + ", value " + state + ": " + data);
	}
}
