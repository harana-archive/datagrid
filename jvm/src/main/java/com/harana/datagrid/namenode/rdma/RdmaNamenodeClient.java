package com.harana.datagrid.namenode.rdma;

import java.net.InetSocketAddress;

import com.harana.datagrid.client.namenode.NamenodeClient;
import com.harana.datagrid.client.namenode.NamenodeConnection;
import com.harana.datagrid.conf.Configuration;
import com.harana.datagrid.rpc.darpc.DaRPCClientEndpoint;
import com.harana.datagrid.rpc.darpc.DaRPCClientGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaNamenodeClient implements NamenodeClient {
	private static final Logger logger = LogManager.getLogger();
	
	private RdmaNamenodeProtocol namenodeProtocol;
	private DaRPCClientGroup<RdmaNamenodeRequest, RdmaNamenodeResponse> namenodeClientGroup;
	
	public RdmaNamenodeClient() {
		this.namenodeProtocol = null;
		this.namenodeClientGroup = null;
	}
	
	public void init(Configuration conf, String[] args) throws Exception{
		RdmaConstants.updateConstants(conf);
		RdmaConstants.verify();
		this.namenodeProtocol = new RdmaNamenodeProtocol();
		this.namenodeClientGroup = DaRPCClientGroup.createClientGroup(namenodeProtocol, 100, RdmaConstants.NAMENODE_RDMA_MAXINLINE, RdmaConstants.NAMENODE_RDMA_RECVQUEUE, RdmaConstants.NAMENODE_RDMA_SENDQUEUE);
		logger.info("rpc group started, recvQueue " + namenodeClientGroup.recvQueueSize());
	}
	
	public void printConf(Logger logger) {
		RdmaConstants.printConf(logger);
	}

	@Override
	public NamenodeConnection connect(InetSocketAddress address) throws Exception {
		DaRPCClientEndpoint<RdmaNamenodeRequest, RdmaNamenodeResponse> namenodeEndopoint = namenodeClientGroup.createEndpoint();
		namenodeEndopoint.connect(address, RdmaConstants.NAMENODE_RDMA_CONNECTTIMEOUT);
		return new RdmaNamenodeConnection(namenodeEndopoint);
	}

	@Override
	public void close() {
		try {
			if (namenodeClientGroup != null) {
				namenodeClientGroup.close();
				namenodeClientGroup = null;
			}
		} catch(Exception e) {
			e.printStackTrace();
			logger.info("Error while closing " + e.getMessage());
		}
	}

}
