package com.harana.datagrid.namenode.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import com.harana.datagrid.client.namenode.NamenodeClient;
import com.harana.datagrid.client.namenode.NamenodeConnection;
import com.harana.datagrid.conf.Configuration;
import com.harana.datagrid.rpc.narpc.NaRPCClientGroup;
import com.harana.datagrid.rpc.narpc.NaRPCEndpoint;
import org.apache.logging.log4j.Logger;

import static com.harana.datagrid.namenode.tcp.TcpNamenodeConstants.*;

public class TcpNamenodeClient implements NamenodeClient {

	private NaRPCClientGroup<TcpNamenodeRequest, TcpNamenodeResponse> clientGroup;
	private LinkedList<TcpNamenodeConnection> allConnections;
	
    public void init(Configuration conf, String[] strings) throws IOException {
    	try {
    		updateConstants(conf);
    		verify();
    		this.clientGroup = new NaRPCClientGroup<>(NAMENODE_TCP_QUEUEDEPTH, NAMENODE_TCP_MESSAGESIZE, true);
    		this.allConnections = new LinkedList<>();
    	} catch(Exception e) {
    		throw new IOException(e);
    	}
    }

    public void printConf(Logger logger) {
    	printConf(logger);
    }

    public NamenodeConnection connect(InetSocketAddress address) throws IOException {
    	try {
    		NaRPCEndpoint<TcpNamenodeRequest, TcpNamenodeResponse> endpoint = clientGroup.createEndpoint();
    		endpoint.connect(address);
    		TcpNamenodeConnection connection = new TcpNamenodeConnection(endpoint);
    		allConnections.add(connection);
    		return connection;
    	} catch(Exception e) {
    		throw new IOException(e);
    	}
    }

	@Override
	public void close() {
		try {
			for (TcpNamenodeConnection connection : allConnections) {
				connection.close();
			}
		} catch(Exception e) {}
	}
}