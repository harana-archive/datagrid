package com.harana.datagrid.storage.tcp;

import java.io.IOException;

import com.harana.datagrid.CrailBufferCache;
import com.harana.datagrid.CrailStatistics;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.storage.StorageClient;
import com.harana.datagrid.storage.StorageEndpoint;
import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

import com.ibm.narpc.NaRPCClientGroup;
import com.ibm.narpc.NaRPCEndpoint;

public class TcpStorageClient implements StorageClient {
	private NaRPCClientGroup<TcpStorageRequest, TcpStorageResponse> clientGroup;

	@Override
	public void init(CrailStatistics statistics, CrailBufferCache bufferCache, CrailConfiguration conf, String[] args)
			throws IOException {
		TcpStorageConstants.updateConstants(conf);

		this.clientGroup = new NaRPCClientGroup<TcpStorageRequest, TcpStorageResponse>(TcpStorageConstants.STORAGE_TCP_QUEUE_DEPTH, (int) CrailConstants.BLOCK_SIZE*2, false);
	}

	@Override
	public void printConf(Logger logger) {
		TcpStorageConstants.printConf(logger);
	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		try {
			NaRPCEndpoint<TcpStorageRequest, TcpStorageResponse> narpcEndpoint = clientGroup.createEndpoint();
			TcpStorageEndpoint endpoint = new TcpStorageEndpoint(narpcEndpoint);
			endpoint.connect(CrailUtils.datanodeInfo2SocketAddr(info));
			return endpoint;
		} catch(Exception e){
			throw new IOException(e);
		}
	}

}
