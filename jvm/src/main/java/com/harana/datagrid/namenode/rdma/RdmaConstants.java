package com.harana.datagrid.namenode.rdma;

import java.io.IOException;

import com.harana.datagrid.conf.DatagridConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaConstants {
	private static final Logger logger = LogManager.getLogger();
	
	public static final String NAMENODE_RDMA_POLLING_KEY = "namenode.rdma.polling";
	public static boolean NAMENODE_RDMA_POLLING = false;
	
	public static final String NAMENODE_RDMA_TYPE_KEY = "namenode.rdma.type";
	public static String NAMENODE_RDMA_TYPE = "passive";
	
	public static final String NAMENODE_RDMA_AFFINITY_KEY = "namenode.rdma.affinity";
	public static String NAMENODE_RDMA_AFFINITY = "1";
	
	public static final String NAMENODE_RDMA_MAXINLINE_KEY = "namenode.rdma.maxinline";
	public static int NAMENODE_RDMA_MAXINLINE = 0;

	public static final String NAMENODE_RDMA_RECVQUEUE_KEY = "namenode.rdma.recvQueue";
	public static int NAMENODE_RDMA_RECVQUEUE = 32;
	
	public static final String NAMENODE_RDMA_SENDQUEUE_KEY = "namenode.rdma.sendQueue";
	public static int NAMENODE_RDMA_SENDQUEUE = 32;
	
	public static final String NAMENODE_RDMA_POLLSIZE_KEY = "namenode.rdma.pollsize";
	public static int NAMENODE_RDMA_POLLSIZE = NAMENODE_RDMA_RECVQUEUE;
	
	public static final String NAMENODE_RDMA_CLUSTERSIZE_KEY = "namenode.rdma.clustersize";
	public static int NAMENODE_RDMA_CLUSTERSIZE = 128;
	
	public static final String NAMENODE_RDMA_BACKLOG_KEY = "namenode.rdma.backlog";
	public static int NAMENODE_RDMA_BACKLOG = 100;
	
	public static final String NAMENODE_RDMA_CONNECTTIMEOUT_KEY = "namenode.rdma.connecttimeout";
	public static int NAMENODE_RDMA_CONNECTTIMEOUT = 1000;
	
	public static void updateConstants(DatagridConfiguration conf) {
		if (conf.get(NAMENODE_RDMA_POLLING_KEY) != null) {
			NAMENODE_RDMA_POLLING = conf.getBoolean(NAMENODE_RDMA_POLLING_KEY, false);
		}			
		if (conf.get(NAMENODE_RDMA_TYPE_KEY) != null) {
			NAMENODE_RDMA_TYPE = conf.get(NAMENODE_RDMA_TYPE_KEY);
		}	
		if (conf.get(NAMENODE_RDMA_AFFINITY_KEY) != null) {
			NAMENODE_RDMA_AFFINITY = conf.get(NAMENODE_RDMA_AFFINITY_KEY);
		}	
		if (conf.get(NAMENODE_RDMA_MAXINLINE_KEY) != null) {
			NAMENODE_RDMA_MAXINLINE = Integer.parseInt(conf.get(NAMENODE_RDMA_MAXINLINE_KEY));
		}	
		if (conf.get(NAMENODE_RDMA_RECVQUEUE_KEY) != null) {
			NAMENODE_RDMA_RECVQUEUE = Integer.parseInt(conf.get(NAMENODE_RDMA_RECVQUEUE_KEY));
		}
		if (conf.get(NAMENODE_RDMA_SENDQUEUE_KEY) != null) {
			NAMENODE_RDMA_SENDQUEUE = Integer.parseInt(conf.get(NAMENODE_RDMA_SENDQUEUE_KEY));
		}		
		if (conf.get(NAMENODE_RDMA_POLLSIZE_KEY) != null) {
			NAMENODE_RDMA_POLLSIZE = Integer.parseInt(conf.get(NAMENODE_RDMA_POLLSIZE_KEY));
		}	
		if (conf.get(NAMENODE_RDMA_CLUSTERSIZE_KEY) != null) {
			NAMENODE_RDMA_CLUSTERSIZE = Integer.parseInt(conf.get(NAMENODE_RDMA_CLUSTERSIZE_KEY));
		}	
		if (conf.get(NAMENODE_RDMA_BACKLOG_KEY) != null) {
			NAMENODE_RDMA_BACKLOG = Integer.parseInt(conf.get(NAMENODE_RDMA_BACKLOG_KEY));
		}	
		if (conf.get(NAMENODE_RDMA_CONNECTTIMEOUT_KEY) != null) {
			NAMENODE_RDMA_CONNECTTIMEOUT = Integer.parseInt(conf.get(NAMENODE_RDMA_CONNECTTIMEOUT_KEY));
		}			
	}
	
	public static void verify() throws IOException {
		if (!RdmaConstants.NAMENODE_RDMA_TYPE.equalsIgnoreCase("passive") && !RdmaConstants.NAMENODE_RDMA_TYPE.equalsIgnoreCase("active")) {
			throw new IOException("namenode.rdma.type must be either <active> or <passive>, found " + RdmaConstants.NAMENODE_RDMA_TYPE);
		}		
	}

	public static void printConf(Logger logger) {
		logger.info(NAMENODE_RDMA_POLLING_KEY + " " + NAMENODE_RDMA_POLLING);
		logger.info(NAMENODE_RDMA_TYPE_KEY + " " + NAMENODE_RDMA_TYPE);
		logger.info(NAMENODE_RDMA_AFFINITY_KEY + " " + NAMENODE_RDMA_AFFINITY);
		logger.info(NAMENODE_RDMA_MAXINLINE_KEY + " " + NAMENODE_RDMA_MAXINLINE);
		logger.info(NAMENODE_RDMA_RECVQUEUE_KEY + " " + NAMENODE_RDMA_RECVQUEUE);
		logger.info(NAMENODE_RDMA_SENDQUEUE_KEY + " " + NAMENODE_RDMA_SENDQUEUE);
		logger.info(NAMENODE_RDMA_POLLSIZE_KEY + " " + NAMENODE_RDMA_POLLSIZE);
		logger.info(NAMENODE_RDMA_CLUSTERSIZE_KEY + " " + NAMENODE_RDMA_CLUSTERSIZE);
		logger.info(NAMENODE_RDMA_BACKLOG_KEY + " " + NAMENODE_RDMA_BACKLOG);
		logger.info(NAMENODE_RDMA_CONNECTTIMEOUT_KEY + " " + NAMENODE_RDMA_CONNECTTIMEOUT);
	}	
}
