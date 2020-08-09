package com.harana.datagrid.namenode.rpc.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcServer;
import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCServerChannel;
import com.ibm.narpc.NaRPCServerEndpoint;
import com.ibm.narpc.NaRPCServerGroup;

public class TcpNameNodeServer extends RpcServer {
	private static final Logger logger = LogManager.getLogger();

	private TcpRpcDispatcher dispatcher;
	private NaRPCServerGroup<TcpNameNodeRequest, TcpNameNodeResponse> serverGroup;
	private NaRPCServerEndpoint<TcpNameNodeRequest, TcpNameNodeResponse> serverEndpoint;

	public TcpNameNodeServer(RpcNameNodeService service) throws IOException {
		this.dispatcher = new TcpRpcDispatcher(service);
	}

	@Override
	public void init(CrailConfiguration conf, String[] arg1) throws Exception {
		TcpRpcConstants.updateConstants(conf);
		TcpRpcConstants.verify();
		this.serverGroup = new NaRPCServerGroup<TcpNameNodeRequest, TcpNameNodeResponse>(
				dispatcher, TcpRpcConstants.NAMENODE_TCP_QUEUEDEPTH,
				TcpRpcConstants.NAMENODE_TCP_MESSAGESIZE, true, TcpRpcConstants.NAMENODE_TCP_CORES);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		InetSocketAddress inetSocketAddress = CrailUtils.getNameNodeAddress();
		serverEndpoint.bind(inetSocketAddress);
	}

	@Override
	public void printConf(Logger logger) {
		TcpRpcConstants.printConf(logger);
	}

	public void run() {
		try {
			while (true) {
				NaRPCServerChannel endpoint = serverEndpoint.accept();
				logger.info("new connection from " + endpoint.address());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
