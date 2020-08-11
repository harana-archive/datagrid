package com.harana.datagrid.datanode.object;

import com.harana.datagrid.DatagridBufferCache;
import com.harana.datagrid.DatagridStatistics;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.conf.DatagridConfiguration;
import com.harana.datagrid.datanode.DatanodeServer;
import com.harana.datagrid.datanode.DatanodeTier;
import com.harana.datagrid.datanode.object.client.ObjectStoreDataNodeEndpoint;
import com.harana.datagrid.datanode.object.client.ObjectStoreMetadataClientGroup;
import com.harana.datagrid.datanode.object.server.ObjectStoreServer;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;


public class ObjectStorageTier implements DatanodeTier {
	private static final Logger logger = LogManager.getLogger();

	private ObjectStoreMetadataClientGroup metadataClientGroup = null;
	private final ObjectStoreServer storageServer = null;
	private DatagridStatistics statistics;
	private DatagridBufferCache bufferCache;

	@Override
	public void init(DatagridStatistics stats, DatagridBufferCache cache, DatagridConfiguration conf, String[] args) {
		logger.debug("Initializing ObjectStorageTier");
		com.harana.datagrid.datanode.object.ObjectStoreConstants.updateConstants(conf);
		metadataClientGroup = new ObjectStoreMetadataClientGroup();
		boolean initialized = true;
	}

	@Override
	public DatanodeServer launchServer() {
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
	public DatanodeEndpoint createEndpoint(DatanodeInfo dataNodeInfo) throws IOException {
		InetSocketAddress addr = Utils.datanodeInfo2SocketAddr(dataNodeInfo);
		logger.debug("Opening a connection to StorageNode: " + addr.toString());
		return new ObjectStoreDataNodeEndpoint(metadataClientGroup.getClient());
	}
}
