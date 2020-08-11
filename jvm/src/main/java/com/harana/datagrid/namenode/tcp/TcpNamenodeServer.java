package com.harana.datagrid.namenode.tcp;

import com.harana.datagrid.conf.DatagridConfiguration;
import com.harana.datagrid.namenode.NamenodeService;
import com.harana.datagrid.namenode.NamenodeServer;
import com.harana.datagrid.rpc.narpc.NaRPCServerChannel;
import com.harana.datagrid.rpc.narpc.NaRPCServerEndpoint;
import com.harana.datagrid.rpc.narpc.NaRPCServerGroup;
import com.harana.datagrid.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import static com.harana.datagrid.namenode.tcp.TcpNamenodeConstants.*;

public class TcpNamenodeServer implements NamenodeServer {
	private static final Logger logger = LogManager.getLogger();

	private final TcpNamenodeDispatcher dispatcher;
	private NaRPCServerEndpoint<TcpNamenodeRequest, TcpNamenodeResponse> serverEndpoint;

	public TcpNamenodeServer(NamenodeService service) {
		this.dispatcher = new TcpNamenodeDispatcher(service);
	}

	@Override
	public void init(DatagridConfiguration conf, String[] arg1) throws Exception {
		updateConstants(conf);
		verify();
		NaRPCServerGroup<TcpNamenodeRequest, TcpNamenodeResponse> serverGroup = new NaRPCServerGroup<>(dispatcher, NAMENODE_TCP_QUEUEDEPTH, NAMENODE_TCP_MESSAGESIZE, true, NAMENODE_TCP_CORES);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		InetSocketAddress inetSocketAddress = Utils.getNameNodeAddress();
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