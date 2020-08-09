package com.harana.datagrid.storage.tcp;

import java.nio.ByteBuffer;

import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

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
