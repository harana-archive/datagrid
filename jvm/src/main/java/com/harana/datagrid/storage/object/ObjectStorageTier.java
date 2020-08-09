/*
 * Copyright (C) 2015-2018, IBM Corporation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.crail.storage.object;

import com.ibm.crail.storage.object.client.ObjectStoreDataNodeEndpoint;
import com.ibm.crail.storage.object.client.ObjectStoreMetadataClientGroup;
import com.ibm.crail.storage.object.server.ObjectStoreServer;
import com.harana.datagrid.CrailBufferCache;
import com.harana.datagrid.CrailStatistics;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.storage.StorageEndpoint;
import com.harana.datagrid.storage.StorageServer;
import com.harana.datagrid.storage.StorageTier;
import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;


public class ObjectStorageTier implements StorageTier {
	private static final Logger LOG = ObjectStoreUtils.getLogger();

	private ObjectStoreMetadataClientGroup metadataClientGroup = null;
	private ObjectStoreServer storageServer = null;
	private boolean initialized = false;
	private CrailStatistics statistics;
	private CrailBufferCache bufferCache;

	@Override
	public void init(CrailStatistics stats, CrailBufferCache cache, CrailConfiguration conf, String[] args) {
		logger.debug("Initializing ObjectStorageTier");
		ObjectStoreConstants.updateConstants(conf);
		metadataClientGroup = new ObjectStoreMetadataClientGroup();
		this.initialized = true;
		this.statistics = statistics;
		this.bufferCache = bufferCache;
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
