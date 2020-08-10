package com.harana.datagrid.rpc.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.narpc.NaRPCClientGroup;
import com.harana.datagrid.narpc.NaRPCEndpoint;
import com.harana.datagrid.rpc.RpcClient;
import com.harana.datagrid.rpc.RpcConnection;
import org.apache.logging.log4j.Logger;

import static com.harana.datagrid.rpc.tcp.TcpRpcConstants.*;

public class TcpNameNodeClient implements RpcClient {

	private NaRPCClientGroup<TcpNameNodeRequest, TcpNameNodeResponse> clientGroup;
	private LinkedList<TcpRpcConnection> allConnections;
	
    public void init(CrailConfiguration conf, String[] strings) throws IOException {
    	try {
    		updateConstants(conf);
    		verify();
    		this.clientGroup = new NaRPCClientGroup<>(NAMENODE_TCP_QUEUEDEPTH, NAMENODE_TCP_MESSAGESIZE, true);
    		this.allConnections = new LinkedList<>();
    	} catch(Exception e){
    		throw new IOException(e);
    	}
    }

    public void printConf(Logger logger) {
    	printConf(logger);
    }

    /* This function comes from RPCClient interface */
    public RpcConnection connect(InetSocketAddress address) throws IOException {
    	try {
    		NaRPCEndpoint<TcpNameNodeRequest, TcpNameNodeResponse> endpoint = clientGroup.createEndpoint();
    		endpoint.connect(address);
    		TcpRpcConnection connection = new TcpRpcConnection(endpoint);
    		allConnections.add(connection);
    		return connection;
    	} catch(Exception e){
    		throw new IOException(e);
    	}
    }

	@Override
	public void close() {
		try {
			for (TcpRpcConnection connection : allConnections){
				connection.close();
			}
		} catch(Exception e){}
	}
}