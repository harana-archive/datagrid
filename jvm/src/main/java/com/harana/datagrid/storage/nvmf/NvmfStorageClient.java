package com.harana.datagrid.storage.nvmf;

import com.harana.datagrid.CrailBufferCache;
import com.harana.datagrid.CrailStatistics;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.storage.StorageClient;
import com.harana.datagrid.storage.StorageEndpoint;
import com.harana.datagrid.storage.nvmf.client.NvmfStorageEndpoint;
import com.harana.datagrid.storage.nvmf.jvnmf.Nvme;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class NvmfStorageClient implements StorageClient {
	private static final Logger logger = LogManager.getLogger();
	private static Nvme nvme;
	private boolean initialized;
	private volatile boolean closing;
	private final Thread keepAliveThread;
	private List<NvmfStorageEndpoint> endpoints;
	private CrailStatistics statistics;
	private CrailBufferCache bufferCache;

	public NvmfStorageClient() {
		this.initialized = false;
		this.endpoints = new CopyOnWriteArrayList<>();
		this.closing = false;
		this.keepAliveThread = new Thread(() -> {
			while (!closing) {
				for (NvmfStorageEndpoint endpoint : endpoints) {
					try {
						endpoint.keepAlive();
					} catch (IOException e) {
						e.printStackTrace();
						return;
					}
				}
				try {
					Thread.sleep(NvmfStorageConstants.KEEP_ALIVE_INTERVAL_MS);
				} catch (InterruptedException e) {
					return;
				}
			}
		});
		this.keepAliveThread.setDaemon(true);
	}

	boolean isAlive() {
		return keepAliveThread.isAlive();
	}

	public void init(CrailStatistics statistics, CrailBufferCache bufferCache, CrailConfiguration crailConfiguration, String[] args) throws IOException {
		if (initialized) {
			throw new IOException("NvmfStorageTier already initialized");
		}
		initialized = true;
		this.statistics = statistics;
		this.bufferCache = bufferCache;
		logger.info("Initialize Nvmf storage client");
		NvmfStorageConstants.parseCmdLine(crailConfiguration, args);
		keepAliveThread.start();
	}

	public void printConf(Logger logger) {
		NvmfStorageConstants.printConf(logger);
	}

	private static Nvme getEndpointGroup() {
		if (nvme == null) {
			if (NvmfStorageConstants.HOST_NQN == null) {
				nvme = new Nvme();
			} else {
				nvme = new Nvme(NvmfStorageConstants.HOST_NQN);
			}
		}
		return nvme;
	}

	public synchronized StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		if (!isAlive()) {
			throw new IOException("Storage client is not alive");
		}
		NvmfStorageEndpoint endpoint = new NvmfStorageEndpoint(getEndpointGroup(), info, statistics, bufferCache);
		endpoints.add(endpoint);
		return endpoint;
	}

	public void close() throws Exception {
		if (!closing) {
			closing = true;
			keepAliveThread.interrupt();
			keepAliveThread.join();
			for (StorageEndpoint endpoint : endpoints) {
				endpoint.close();
			}
		}
	}
}