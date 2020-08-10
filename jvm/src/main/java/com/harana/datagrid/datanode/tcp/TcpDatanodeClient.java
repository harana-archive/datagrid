package com.harana.datagrid.datanode.tcp;

import java.io.IOException;

import com.harana.datagrid.BufferCache;
import com.harana.datagrid.Statistics;
import com.harana.datagrid.client.datanode.DatanodeClient;
import com.harana.datagrid.conf.Configuration;
import com.harana.datagrid.conf.Constants;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.rpc.narpc.NaRPCClientGroup;
import com.harana.datagrid.rpc.narpc.NaRPCEndpoint;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.utils.Utils;
import org.apache.logging.log4j.Logger;

public class TcpDatanodeClient implements DatanodeClient {
	private NaRPCClientGroup<TcpDatanodeRequest, TcpDatanodeResponse> clientGroup;

	@Override
	public void init(Statistics statistics, BufferCache bufferCache, Configuration conf, String[] args) {
		TcpDatanodeConstants.updateConstants(conf);
		this.clientGroup = new NaRPCClientGroup<>(TcpDatanodeConstants.STORAGE_TCP_QUEUE_DEPTH, (int) Constants.BLOCK_SIZE * 2, false);
	}

	@Override
	public void printConf(Logger logger) {
		TcpDatanodeConstants.printConf(logger);
	}

	@Override
	public void close() {
	}

	@Override
	public DatanodeEndpoint createEndpoint(DatanodeInfo info) throws IOException {
		try {
			NaRPCEndpoint<TcpDatanodeRequest, TcpDatanodeResponse> narpcEndpoint = clientGroup.createEndpoint();
			TcpDatanodeEndpoint endpoint = new TcpDatanodeEndpoint(narpcEndpoint);
			endpoint.connect(Utils.datanodeInfo2SocketAddr(info));
			return endpoint;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}