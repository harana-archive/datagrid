package com.harana.datagrid.rpc.tcp;

import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.narpc.NaRPCServerChannel;
import com.harana.datagrid.narpc.NaRPCServerEndpoint;
import com.harana.datagrid.narpc.NaRPCServerGroup;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcServer;
import com.harana.datagrid.utils.CrailUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import static com.harana.datagrid.rpc.tcp.TcpRpcConstants.*;

public class TcpNameNodeServer extends RpcServer {
	private static final Logger logger = LogManager.getLogger();

	private TcpRpcDispatcher dispatcher;
	private NaRPCServerGroup<TcpNameNodeRequest, TcpNameNodeResponse> serverGroup;
	private NaRPCServerEndpoint<TcpNameNodeRequest, TcpNameNodeResponse> serverEndpoint;

	public TcpNameNodeServer(RpcNameNodeService service) {
		this.dispatcher = new TcpRpcDispatcher(service);
	}

	@Override
	public void init(CrailConfiguration conf, String[] arg1) throws Exception {
		updateConstants(conf);
		verify();
		this.serverGroup = new NaRPCServerGroup<>(dispatcher, NAMENODE_TCP_QUEUEDEPTH, NAMENODE_TCP_MESSAGESIZE, true, NAMENODE_TCP_CORES);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		InetSocketAddress inetSocketAddress = CrailUtils.getNameNodeAddress();
		serverEndpoint.bind(inetSocketAddress);
	}

	@Override
	public void printConf(Logger logger) {
		printConf(logger);
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