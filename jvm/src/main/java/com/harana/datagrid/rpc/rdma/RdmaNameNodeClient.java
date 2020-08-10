package com.harana.datagrid.rpc.rdma;

import java.net.InetSocketAddress;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.rpc.RpcClient;
import com.harana.datagrid.rpc.RpcConnection;
import com.harana.datagrid.darpc.DaRPCClientEndpoint;
import com.harana.datagrid.darpc.DaRPCClientGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaNameNodeClient implements RpcClient {
	private static final Logger logger = LogManager.getLogger();
	
	private RdmaNameNodeProtocol namenodeProtocol;
	private DaRPCClientGroup<RdmaNameNodeRequest, RdmaNameNodeResponse> namenodeClientGroup;
	
	public RdmaNameNodeClient(){
		this.namenodeProtocol = null;
		this.namenodeClientGroup = null;
	}
	
	public void init(CrailConfiguration conf, String[] args) throws Exception{
		RdmaConstants.updateConstants(conf);
		RdmaConstants.verify();
		this.namenodeProtocol = new RdmaNameNodeProtocol();
		this.namenodeClientGroup = DaRPCClientGroup.createClientGroup(namenodeProtocol, 100, RdmaConstants.NAMENODE_RDMA_MAXINLINE, RdmaConstants.NAMENODE_RDMA_RECVQUEUE, RdmaConstants.NAMENODE_RDMA_SENDQUEUE);
		logger.info("rpc group started, recvQueue " + namenodeClientGroup.recvQueueSize());
	}
	
	public void printConf(Logger logger){
		RdmaConstants.printConf(logger);
	}

	@Override
	public RpcConnection connect(InetSocketAddress address) throws Exception {
		DaRPCClientEndpoint<RdmaNameNodeRequest, RdmaNameNodeResponse> namenodeEndopoint = namenodeClientGroup.createEndpoint();
		namenodeEndopoint.connect(address, RdmaConstants.NAMENODE_RDMA_CONNECTTIMEOUT);
		return new RdmaNameNodeConnection(namenodeEndopoint);
	}

	@Override
	public void close() {
		try {
			if (namenodeClientGroup != null){
				namenodeClientGroup.close();
				namenodeClientGroup = null;
			}
		} catch(Exception e){
			e.printStackTrace();
			logger.info("Error while closing " + e.getMessage());
		}
	}

}
