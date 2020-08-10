package com.harana.datagrid.rpc.darpc;

import java.net.InetSocketAddress;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.rpc.RpcClient;
import com.harana.datagrid.rpc.RpcConnection;
import com.harana.datagrid.darpc.DaRPCClientEndpoint;
import com.harana.datagrid.darpc.DaRPCClientGroup;
import com.harana.datagrid.rpc.darpc.DaRPCConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DaRPCNameNodeClient implements RpcClient {
	private static final Logger logger = LogManager.getLogger();
	
	private DaRPCNameNodeProtocol namenodeProtocol;
	private DaRPCClientGroup<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeClientGroup;
	
	public DaRPCNameNodeClient(){
		this.namenodeProtocol = null;
		this.namenodeClientGroup = null;
	}
	
	public void init(CrailConfiguration conf, String[] args) throws Exception{
		DaRPCConstants.updateConstants(conf);
		DaRPCConstants.verify();
		this.namenodeProtocol = new DaRPCNameNodeProtocol();
		this.namenodeClientGroup = DaRPCClientGroup.createClientGroup(namenodeProtocol, 100, DaRPCConstants.NAMENODE_DARPC_MAXINLINE, DaRPCConstants.NAMENODE_DARPC_RECVQUEUE, DaRPCConstants.NAMENODE_DARPC_SENDQUEUE);
		logger.info("rpc group started, recvQueue " + namenodeClientGroup.recvQueueSize());
	}
	
	public void printConf(Logger logger){
		DaRPCConstants.printConf(logger);
	}

	@Override
	public RpcConnection connect(InetSocketAddress address) throws Exception {
		DaRPCClientEndpoint<DaRPCNameNodeRequest, DaRPCNameNodeResponse> namenodeEndopoint = namenodeClientGroup.createEndpoint();
		namenodeEndopoint.connect(address, DaRPCConstants.NAMENODE_DARPC_CONNECTTIMEOUT);
		return new DaRPCNameNodeConnection(namenodeEndopoint);
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
