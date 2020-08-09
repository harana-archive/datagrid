package com.harana.datagrid.storage.rdma;

import java.io.IOException;

import com.harana.datagrid.CrailBufferCache;
import com.harana.datagrid.CrailStatistics;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.storage.StorageClient;
import com.harana.datagrid.storage.StorageEndpoint;
import com.harana.datagrid.storage.rdma.client.RdmaStorageActiveEndpointFactory;
import com.harana.datagrid.storage.rdma.client.RdmaStorageActiveGroup;
import com.harana.datagrid.storage.rdma.client.RdmaStoragePassiveEndpointFactory;
import com.harana.datagrid.storage.rdma.client.RdmaStoragePassiveGroup;
import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

public class RdmaStorageClient implements StorageClient {
	private static final Logger logger = LogManager.getLogger();

	private MrCache clientMrCache = null;
	private RdmaStorageGroup clientGroup = null;

	public RdmaStorageClient(){
		this.clientGroup = null;
		this.clientMrCache = null;
	}

	public void init(CrailStatistics statistics, CrailBufferCache bufferCache, CrailConfiguration conf, String[] args)
			throws IOException {
		RdmaConstants.init(conf, args);
	}

	public void printConf(Logger logger){
		RdmaConstants.printConf(logger);
	}

	@Override
	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		if (clientMrCache == null){
			synchronized(this){
				if (clientMrCache == null){
					this.clientMrCache = new MrCache();
				}
			}
		}
		if (clientGroup == null){
			synchronized(this){
				if (clientGroup == null){
					if (RdmaConstants.STORAGE_RDMA_TYPE.equalsIgnoreCase("passive")){
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
		if (clientGroup != null){
			this.clientGroup.close();
		}
	}
}
