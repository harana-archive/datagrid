package com.harana.datagrid.namenode.rpc.tcp;

import java.io.IOException;

import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

public class TcpRpcConstants {
	private static final Logger logger = LogManager.getLogger();
	
	public static final String NAMENODE_TCP_QUEUEDEPTH_KEY = "crail.namenode.tcp.queueDepth";
	public static int NAMENODE_TCP_QUEUEDEPTH = 32;
	
	public static final String NAMENODE_TCP_MESSAGESIZE_KEY = "crail.namenode.tcp.messageSize";
	public static int NAMENODE_TCP_MESSAGESIZE = 512;	
	
	public static final String NAMENODE_TCP_CORES_KEY = "crail.namenode.tcp.cores";
	public static int NAMENODE_TCP_CORES = 1;	
	
	public static void updateConstants(CrailConfiguration conf){
		if (conf.get(NAMENODE_TCP_QUEUEDEPTH_KEY) != null) {
			NAMENODE_TCP_QUEUEDEPTH = Integer.parseInt(conf.get(NAMENODE_TCP_QUEUEDEPTH_KEY));
		}
		if (conf.get(NAMENODE_TCP_MESSAGESIZE_KEY) != null) {
			NAMENODE_TCP_MESSAGESIZE = Integer.parseInt(conf.get(NAMENODE_TCP_MESSAGESIZE_KEY));
		}		
		if (conf.get(NAMENODE_TCP_CORES_KEY) != null) {
			NAMENODE_TCP_CORES = Integer.parseInt(conf.get(NAMENODE_TCP_CORES_KEY));
		}		
	}
	
	public static void verify() throws IOException {
	}

	public static void printConf(Logger logger) {
		logger.info(NAMENODE_TCP_QUEUEDEPTH_KEY + " " + NAMENODE_TCP_QUEUEDEPTH);
		logger.info(NAMENODE_TCP_MESSAGESIZE_KEY + " " + NAMENODE_TCP_MESSAGESIZE);
		logger.info(NAMENODE_TCP_CORES_KEY + " " + NAMENODE_TCP_CORES);
	}	
}
