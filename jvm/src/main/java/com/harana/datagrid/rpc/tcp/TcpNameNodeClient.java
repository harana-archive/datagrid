package com.harana.datagrid.namenode.rpc.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.rpc.RpcClient;
import com.harana.datagrid.rpc.RpcConnection;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCClientGroup;
import com.ibm.narpc.NaRPCEndpoint;

public class TcpNameNodeClient implements RpcClient {
	private NaRPCClientGroup<TcpNameNodeRequest, TcpNameNodeResponse> clientGroup;
	private LinkedList<TcpRpcConnection> allConnections;
	
    public void init(CrailConfiguration conf, String[] strings) throws IOException {
    	try {
    		TcpRpcConstants.updateConstants(conf);
    		TcpRpcConstants.verify();   		
    		this.clientGroup = new NaRPCClientGroup<TcpNameNodeRequest, TcpNameNodeResponse>(TcpRpcConstants.NAMENODE_TCP_QUEUEDEPTH, TcpRpcConstants.NAMENODE_TCP_MESSAGESIZE, true);
    		this.allConnections = new LinkedList<TcpRpcConnection>();
    	} catch(Exception e){
    		throw new IOException(e);
    	}
    }

    public void printConf(Logger logger) {
    	TcpRpcConstants.printConf(logger);
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
		} catch(Exception e){
		}
	}

}
