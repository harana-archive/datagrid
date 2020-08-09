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

package com.ibm.crail.storage.object.server;

import com.ibm.crail.storage.object.ObjectStoreConstants;
import com.ibm.crail.storage.object.ObjectStoreUtils;
import com.ibm.crail.storage.object.client.S3ObjectStoreClient;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.storage.StorageResource;
import com.harana.datagrid.storage.StorageServer;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ObjectStoreServer implements StorageServer {
	private static final Logger LOG = ObjectStoreUtils.getLogger();

	private InetSocketAddress datanodeAddr;
	private long blockID = 0;
	private boolean isAlive = false;
	private long allocated = 0;
	private long alignedSize;
	private int currentStag = 1;
	private boolean initialized;
	private boolean alive;
	private long offset;
	private S3ObjectStoreClient objectStoreClient = null;
	private ObjectStoreMetadataServer metadataServer = null;

	public ObjectStoreServer() {
		super();
	}

	@Override
	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		if (initialized) {
			throw new IOException("ObjectStorageServer already initialized");
		}
		initialized = true;
		if (args != null) {
			for (int i = 0; i < args.length; i++) {
				char flag;
				try {
					if (args[i].charAt(0) == '-') {
						flag = args[i].charAt(1);
					} else {
						loggerwarn("Invalid flag {}", args[i]);
						continue;
					}
					switch (flag) {
						case 't':
							break;
						case 'o':
							String opt;
							if (args[i].length() > 2) {
								opt = args[i].substring(2);
							} else {
								i++;
								opt = args[i];
							}
							String[] split = opt.split("=");
							String key = split[0];
							String val = split[1];
							crailConfiguration.set(key, val);
							logger.info("Set custom option {} = {} ", key, val);
							break;
						default:
							loggerwarn("Unknown flag {}", flag);
							break;
					}
				} catch (Exception e) {
					loggerwarn("Error processing input {}", args[i]);
				}
			}
		}
		ObjectStoreConstants.updateConstants(crailConfiguration);
		ObjectStoreConstants.verify();
		ObjectStoreConstants.parseCmdLine(crailConfiguration, args);

		this.alignedSize = ObjectStoreConstants.STORAGE_LIMIT -
				(ObjectStoreConstants.STORAGE_LIMIT % ObjectStoreConstants.ALLOCATION_SIZE);

		this.alive = false;
	}

	@Override
	public void printConf(Logger logger) {
		ObjectStoreConstants.printConf(logger);
	}

	public void close() throws Exception {
		this.isAlive = false;
		if (metadataServer != null && metadataServer.isAlive()) {
			// stop ObjectStore metadata service
			logger.debug("Closing metadata server");
			metadataServer.close();
		}
		if (objectStoreClient != null) {
			//objectStoreClient.deleteBucket(ObjectStoreConstants.S3_BUCKET_NAME);;
		}
	}

	@Override
	protected void finalize() {
		logger.info("Datanode finalize");
		try {
			close();
		} catch (Exception e) {
			logger.error("Could not close ObjectStoreDataNode. Reason: {}", e);
		}
	}

	@Override
	public StorageResource allocateResource() {
		StorageResource res = null;
		logger.info("Allocating object store blocks");
		if (allocated < alignedSize) {
			long addr = allocated;
			allocated += ObjectStoreConstants.ALLOCATION_SIZE;
			res = StorageResource.createResource(addr, (int) ObjectStoreConstants.ALLOCATION_SIZE, currentStag);
			blockID += ObjectStoreConstants.ALLOCATION_SIZE / CrailConstants.BLOCK_SIZE;
			double perc = (allocated * 100.) / alignedSize;
			currentStag++;
			logger.info("Allocation done : " + perc + "% , allocated " + allocated + " / " + alignedSize);
		}
		return res;
	}

	@Override
	public boolean isAlive() {
		return this.isAlive;
	}

	@Override
	public InetSocketAddress getAddress() {
		return new InetSocketAddress(ObjectStoreConstants.DATANODE, ObjectStoreConstants.DATANODE_PORT);
	}

	public void run() {
		isAlive = true;
		try {
			setup();
		} catch (Exception e) {
			logger.error("Could not set up S3 bucket", e);
			return;
		}
		metadataServer = new ObjectStoreMetadataServer();
		metadataServer.start();

		logger.info("ObjectStorageServer started at " + getAddress());
		while (isAlive) {
			try {
				Thread.sleep(1000 /* ms */);
				//endpoint.keepAlive();
			} catch (Exception e) {
				e.printStackTrace();
				isAlive = false;
			}
		}
		try {
			cleanup();
		} catch (Exception e) {
			logger.error("Could not clean up Crail S3 objects", e);
		}
	}

	private void setup() throws Exception {
		objectStoreClient = new S3ObjectStoreClient();
		if (!objectStoreClient.createBucket(ObjectStoreConstants.S3_BUCKET_NAME)) {
			loggerwarn("Could not create or confirm existence of bucket {}", ObjectStoreConstants.S3_BUCKET_NAME);
		}
	}

	private void cleanup() {
		if (ObjectStoreConstants.CLEANUP_ON_EXIT) {
			objectStoreClient.deleteObjectsWithPrefix(ObjectStoreConstants.OBJECT_PREFIX);
			// TODO: delete also bucket? Makes sense if we created the bucket or if the bucket is empty at the end
		}
	}
}
