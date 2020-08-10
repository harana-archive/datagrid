package com.harana.datagrid.namenode.tcp;

import com.harana.datagrid.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpNamenodeConstants {
	private static final Logger logger = LogManager.getLogger();
	
	public static final String NAMENODE_TCP_QUEUEDEPTH_KEY = "crail.namenode.tcp.queueDepth";
	public static int NAMENODE_TCP_QUEUEDEPTH = 32;
	
	public static final String NAMENODE_TCP_MESSAGESIZE_KEY = "crail.namenode.tcp.messageSize";
	public static int NAMENODE_TCP_MESSAGESIZE = 512;	
	
	public static final String NAMENODE_TCP_CORES_KEY = "crail.namenode.tcp.cores";
	public static int NAMENODE_TCP_CORES = 1;	
	
	public static void updateConstants(Configuration conf) {
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
	
	public static void verify() {
	}

	public static void printConf(Logger logger) {
		logger.info(NAMENODE_TCP_QUEUEDEPTH_KEY + " " + NAMENODE_TCP_QUEUEDEPTH);
		logger.info(NAMENODE_TCP_MESSAGESIZE_KEY + " " + NAMENODE_TCP_MESSAGESIZE);
		logger.info(NAMENODE_TCP_CORES_KEY + " " + NAMENODE_TCP_CORES);
	}	
}