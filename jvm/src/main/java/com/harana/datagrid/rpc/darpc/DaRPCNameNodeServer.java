package com.harana.datagrid.namenode.rpc.darpc;

import java.net.InetSocketAddress;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcServer;
import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

import com.harana.datagrid.darpc.DaRPCServerEndpoint;
import com.harana.datagrid.darpc.DaRPCServerGroup;
import com.harana.datagrid.rdma.RdmaServerEndpoint;

public class DaRPCNameNodeServer extends RpcServer {
	private static final Logger logger = LogManager.getLogger();
	
	private RpcNameNodeService service;
	private DaRPCServerGroup<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeServerGroup;
	private RdmaServerEndpoint<DaRPCServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse>> namenodeServerEp;	
	
	public DaRPCNameNodeServer(RpcNameNodeService service){
		this.service = service;
		this.namenodeServerEp = null;
		this.namenodeServerGroup = null;
	}	

	public void init(CrailConfiguration conf, String[] args) throws Exception{
		DaRPCConstants.updateConstants(conf);
		DaRPCConstants.verify();
		
		String _clusterAffinities[] = DaRPCConstants.NAMENODE_DARPC_AFFINITY.split(",");
		long clusterAffinities[] = new long[_clusterAffinities.length];
		for (int i = 0; i < clusterAffinities.length; i++){
			int affinity = Integer.decode(_clusterAffinities[i]).intValue();
			clusterAffinities[i] = 1L << affinity;
		}
		DaRPCServiceDispatcher darpcService = new DaRPCServiceDispatcher(service);
		this.namenodeServerGroup = DaRPCServerGroup.createServerGroup(darpcService, clusterAffinities, -1, DaRPCConstants.NAMENODE_DARPC_MAXINLINE, DaRPCConstants.NAMENODE_DARPC_POLLING, DaRPCConstants.NAMENODE_DARPC_RECVQUEUE, DaRPCConstants.NAMENODE_DARPC_SENDQUEUE, DaRPCConstants.NAMENODE_DARPC_POLLSIZE, DaRPCConstants.NAMENODE_DARPC_CLUSTERSIZE);
		logger.info("rpc group started, recvQueue " + namenodeServerGroup.recvQueueSize());
		this.namenodeServerEp = namenodeServerGroup.createServerEndpoint();		
	}
	
	public void printConf(Logger logger){
		DaRPCConstants.printConf(logger);
	}

	@Override
	public void run() {
		try {
			InetSocketAddress addr = CrailUtils.getNameNodeAddress();
			namenodeServerEp.bind(addr, DaRPCConstants.NAMENODE_DARPC_BACKLOG);
			logger.info("opened server at " + addr);
			while (true) {
				DaRPCServerEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> clientEndpoint = namenodeServerEp.accept();
				logger.info("accepting RPC connection, qpnum " + clientEndpoint.getQp().getQp_num());
			}
		} catch(Exception e){
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

}
