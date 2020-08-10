package com.harana.datagrid.rpc.rdma;

import java.net.InetSocketAddress;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcServer;
import com.harana.datagrid.darpc.DaRPCServerEndpoint;
import com.harana.datagrid.darpc.DaRPCServerGroup;
import com.harana.datagrid.rdma.RdmaServerEndpoint;
import com.harana.datagrid.utils.CrailUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaNameNodeServer extends RpcServer {
	private static final Logger logger = LogManager.getLogger();
	
	private RpcNameNodeService service;
	private DaRPCServerGroup<RdmaNameNodeRequest, RdmaNameNodeResponse> namenodeServerGroup;
	private RdmaServerEndpoint<DaRPCServerEndpoint<RdmaNameNodeRequest, RdmaNameNodeResponse>> namenodeServerEp;
	
	public RdmaNameNodeServer(RpcNameNodeService service){
		this.service = service;
		this.namenodeServerEp = null;
		this.namenodeServerGroup = null;
	}	

	public void init(CrailConfiguration conf, String[] args) throws Exception{
		RdmaConstants.updateConstants(conf);
		RdmaConstants.verify();
		
		String[] _clusterAffinities = RdmaConstants.NAMENODE_RDMA_AFFINITY.split(",");
		long[] clusterAffinities = new long[_clusterAffinities.length];
		for (int i = 0; i < clusterAffinities.length; i++){
			int affinity = Integer.decode(_clusterAffinities[i]);
			clusterAffinities[i] = 1L << affinity;
		}

		RdmaServiceDispatcher rdmaService = new RdmaServiceDispatcher(service);
		this.namenodeServerGroup = DaRPCServerGroup.createServerGroup(rdmaService, clusterAffinities, -1, RdmaConstants.NAMENODE_RDMA_MAXINLINE, RdmaConstants.NAMENODE_RDMA_POLLING, RdmaConstants.NAMENODE_RDMA_RECVQUEUE, RdmaConstants.NAMENODE_RDMA_SENDQUEUE, RdmaConstants.NAMENODE_RDMA_POLLSIZE, RdmaConstants.NAMENODE_RDMA_CLUSTERSIZE);
		logger.info("rpc group started, recvQueue " + namenodeServerGroup.recvQueueSize());
		this.namenodeServerEp = namenodeServerGroup.createServerEndpoint();		
	}
	
	public void printConf(Logger logger){
		RdmaConstants.printConf(logger);
	}

	@Override
	public void run() {
		try {
			InetSocketAddress addr = CrailUtils.getNameNodeAddress();
			namenodeServerEp.bind(addr, RdmaConstants.NAMENODE_RDMA_BACKLOG);
			logger.info("opened server at " + addr);
			while (true) {
				DaRPCServerEndpoint<RdmaNameNodeRequest, RdmaNameNodeResponse> clientEndpoint = namenodeServerEp.accept();
				logger.info("accepting RPC connection, qpnum " + clientEndpoint.getQp().getQp_num());
			}
		} catch(Exception e){
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

}
