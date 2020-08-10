package com.harana.datagrid.storage.object;

import com.harana.datagrid.utils.CrailUtils;
import com.harana.datagrid.storage.object.client.ObjectStoreDataNodeEndpoint;
import com.harana.datagrid.storage.object.client.ObjectStoreMetadataClientGroup;
import com.harana.datagrid.storage.object.server.ObjectStoreServer;
import com.harana.datagrid.CrailBufferCache;
import com.harana.datagrid.CrailStatistics;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.storage.StorageEndpoint;
import com.harana.datagrid.storage.StorageServer;
import com.harana.datagrid.storage.StorageTier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;


public class ObjectStorageTier implements StorageTier {
	private static final Logger logger = LogManager.getLogger();

	private ObjectStoreMetadataClientGroup metadataClientGroup = null;
	private ObjectStoreServer storageServer = null;
	private boolean initialized = false;
	private CrailStatistics statistics;
	private CrailBufferCache bufferCache;

	@Override
	public void init(CrailStatistics stats, CrailBufferCache cache, CrailConfiguration conf, String[] args) {
		logger.debug("Initializing ObjectStorageTier");
		com.harana.datagrid.storage.object.ObjectStoreConstants.updateConstants(conf);
		metadataClientGroup = new ObjectStoreMetadataClientGroup();
		this.initialized = true;
	}

	@Override
	public StorageServer launchServer() {
		logger.info("Initializing ObjectStore Tier");
		return new ObjectStoreServer();
	}

	@Override
	public void printConf(Logger logger) {
		ObjectStoreConstants.printConf(logger);
	}

	@Override
	public void close() throws Exception {
		logger.info("Closing ObjectStorageTier");
		if (metadataClientGroup != null) {
			// stop ObjectStore metadata service
			logger.debug("Closing metadata client group");
			metadataClientGroup.closeClientGroup();
		}
		if (storageServer != null && storageServer.isAlive()) {
			// stop ObjectStore metadata service
			logger.debug("Closing metadata server");
			storageServer.close();
		}
	}

	@Override
	public StorageEndpoint createEndpoint(DataNodeInfo dataNodeInfo) throws IOException {
		InetSocketAddress addr = CrailUtils.datanodeInfo2SocketAddr(dataNodeInfo);
		logger.debug("Opening a connection to StorageNode: " + addr.toString());
		return new ObjectStoreDataNodeEndpoint(metadataClientGroup.getClient());
	}
}
