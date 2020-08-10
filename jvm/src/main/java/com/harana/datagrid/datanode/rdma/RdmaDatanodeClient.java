package com.harana.datagrid.datanode.rdma;

import java.io.IOException;

import com.harana.datagrid.BufferCache;
import com.harana.datagrid.Statistics;
import com.harana.datagrid.client.datanode.DatanodeClient;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.conf.Configuration;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.datanode.rdma.client.RdmaStorageActiveEndpointFactory;
import com.harana.datagrid.datanode.rdma.client.RdmaStorageActiveGroup;
import com.harana.datagrid.datanode.rdma.client.RdmaStoragePassiveEndpointFactory;
import com.harana.datagrid.datanode.rdma.client.RdmaStoragePassiveGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaDatanodeClient implements DatanodeClient {
	private static final Logger logger = LogManager.getLogger();

	private MrCache clientMrCache;
	private RdmaDatanodeGroup clientGroup;

	public RdmaDatanodeClient() {
		this.clientGroup = null;
		this.clientMrCache = null;
	}

	public void init(Statistics statistics, BufferCache bufferCache, Configuration conf, String[] args) throws IOException {
		RdmaConstants.init(conf, args);
	}

	public void printConf(Logger logger) {
		RdmaConstants.printConf(logger);
	}

	@Override
	public DatanodeEndpoint createEndpoint(DatanodeInfo info) throws IOException {
		if (clientMrCache == null) {
			synchronized(this) {
				if (clientMrCache == null) {
					this.clientMrCache = new MrCache();
				}
			}
		}
		if (clientGroup == null) {
			synchronized(this) {
				if (clientGroup == null) {
					if (RdmaConstants.STORAGE_RDMA_TYPE.equalsIgnoreCase("passive")) {
						logger.info("passive data client ");
						RdmaStoragePassiveGroup _endpointGroup = new RdmaStoragePassiveGroup(100, RdmaConstants.STORAGE_RDMA_QUEUESIZE, 4, RdmaConstants.STORAGE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaStoragePassiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					} else {
						logger.info("active data client ");
						RdmaStorageActiveGroup _endpointGroup = new RdmaStorageActiveGroup(100, false, RdmaConstants.STORAGE_RDMA_QUEUESIZE, 4, RdmaConstants.STORAGE_RDMA_QUEUESIZE*2, clientMrCache);
						_endpointGroup.init(new RdmaStorageActiveEndpointFactory(_endpointGroup));
						this.clientGroup = _endpointGroup;
					}
				}
			}
		}

		return clientGroup.createEndpoint(info);
	}

	public void close() throws Exception {
		if (clientGroup != null) {
			this.clientGroup.close();
		}
	}
}