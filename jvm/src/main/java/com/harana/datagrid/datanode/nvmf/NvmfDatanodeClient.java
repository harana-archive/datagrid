package com.harana.datagrid.datanode.nvmf;

import com.harana.datagrid.DatagridBufferCache;
import com.harana.datagrid.DatagridStatistics;
import com.harana.datagrid.client.datanode.DatanodeClient;
import com.harana.datagrid.conf.DatagridConfiguration;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.datanode.nvmf.client.NvmfDatanodeEndpoint;
import com.harana.datagrid.datanode.nvmf.jnvmf.Nvme;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class NvmfDatanodeClient implements DatanodeClient {
	private static final Logger logger = LogManager.getLogger();
	private static Nvme nvme;
	private boolean initialized;
	private volatile boolean closing;
	private final Thread keepAliveThread;
	private final List<NvmfDatanodeEndpoint> endpoints;
	private DatagridStatistics statistics;
	private DatagridBufferCache bufferCache;

	public NvmfDatanodeClient() {
		this.initialized = false;
		this.endpoints = new CopyOnWriteArrayList<>();
		this.closing = false;
		this.keepAliveThread = new Thread(() -> {
			while (!closing) {
				for (NvmfDatanodeEndpoint endpoint : endpoints) {
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

	public void init(DatagridStatistics statistics, DatagridBufferCache bufferCache, DatagridConfiguration conf, String[] args) throws IOException {
		if (initialized) {
			throw new IOException("NvmfStorageTier already initialized");
		}
		initialized = true;
		this.statistics = statistics;
		this.bufferCache = bufferCache;
		logger.info("Initialize Nvmf storage client");
		NvmfStorageConstants.parseCmdLine(conf, args);
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

	public synchronized DatanodeEndpoint createEndpoint(DatanodeInfo info) throws IOException {
		if (!isAlive()) {
			throw new IOException("Storage client is not alive");
		}
		NvmfDatanodeEndpoint endpoint = new NvmfDatanodeEndpoint(getEndpointGroup(), info, statistics, bufferCache);
		endpoints.add(endpoint);
		return endpoint;
	}

	public void close() throws Exception {
		if (!closing) {
			closing = true;
			keepAliveThread.interrupt();
			keepAliveThread.join();
			for (DatanodeEndpoint endpoint : endpoints) {
				endpoint.close();
			}
		}
	}
}