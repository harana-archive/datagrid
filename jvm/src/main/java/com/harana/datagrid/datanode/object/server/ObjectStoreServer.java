package com.harana.datagrid.datanode.object.server;

import com.harana.datagrid.conf.DatagridConfiguration;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.datanode.object.ObjectStoreConstants;
import com.harana.datagrid.datanode.object.client.S3ObjectStoreClient;
import com.harana.datagrid.datanode.DatanodeResource;
import com.harana.datagrid.datanode.DatanodeServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;

public class ObjectStoreServer implements DatanodeServer {
	private static final Logger logger = LogManager.getLogger();

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
	public void init(DatagridConfiguration conf, String[] args) throws IOException {
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
						logger.warn("Invalid flag {}", args[i]);
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
							conf.set(key, val);
							logger.info("Set custom option {} = {} ", key, val);
							break;
						default:
							logger.warn("Unknown flag {}", flag);
							break;
					}
				} catch (Exception e) {
					logger.warn("Error processing input {}", args[i]);
				}
			}
		}
		ObjectStoreConstants.updateConstants(conf);
		ObjectStoreConstants.verify();
		ObjectStoreConstants.parseCmdLine(conf, args);

		this.alignedSize = ObjectStoreConstants.STORAGE_LIMIT - (ObjectStoreConstants.STORAGE_LIMIT % ObjectStoreConstants.ALLOCATION_SIZE);
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
	public DatanodeResource allocateResource() {
		DatanodeResource res = null;
		logger.info("Allocating object store blocks");
		if (allocated < alignedSize) {
			long addr = allocated;
			allocated += ObjectStoreConstants.ALLOCATION_SIZE;
			res = DatanodeResource.createResource(addr, (int) ObjectStoreConstants.ALLOCATION_SIZE, currentStag);
			blockID += ObjectStoreConstants.ALLOCATION_SIZE / DatagridConstants.BLOCK_SIZE;
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
			logger.error("Could not clean up  S3 objects", e);
		}
	}

	private void setup() throws Exception {
		objectStoreClient = new S3ObjectStoreClient();
		if (!objectStoreClient.createBucket(ObjectStoreConstants.S3_BUCKET_NAME)) {
			logger.warn("Could not create or confirm existence of bucket {}", ObjectStoreConstants.S3_BUCKET_NAME);
		}
	}

	private void cleanup() {
		if (ObjectStoreConstants.CLEANUP_ON_EXIT) {
			objectStoreClient.deleteObjectsWithPrefix(ObjectStoreConstants.OBJECT_PREFIX);
			// TODO: delete also bucket? Makes sense if we created the bucket or if the bucket is empty at the end
		}
	}
}
