package com.harana.datagrid.datanode.rdma;

import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ConcurrentHashMap;

import com.harana.datagrid.conf.Configuration;
import com.harana.datagrid.datanode.DatanodeResource;
import com.harana.datagrid.datanode.DatanodeServer;
import com.harana.datagrid.datanode.DatanodeUtils;
import com.harana.datagrid.rdma.*;
import com.harana.datagrid.rdma.verbs.IbvMr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaDatanodeServer implements Runnable, DatanodeServer {
	private static final Logger logger = LogManager.getLogger();
	
	private InetSocketAddress serverAddr;
	private RdmaActiveEndpointGroup<RdmaDatanodeEndpoint> datanodeGroup;
	private RdmaServerEndpoint<RdmaDatanodeEndpoint> datanodeServerEndpoint;
	private final ConcurrentHashMap<Integer, RdmaEndpoint> allEndpoints;
	private boolean isAlive;
	
	private String dataDirPath;
	private long allocatedSize;
	private int fileCount;
	
	public RdmaDatanodeServer() {
		this.isAlive = false;
		this.serverAddr = null;
		this.datanodeGroup = null;
		this.datanodeServerEndpoint = null;
		this.allEndpoints = new ConcurrentHashMap<>();
	}
	
	public void init(Configuration conf, String[] args) throws Exception {
		RdmaConstants.init(conf, args);

		this.serverAddr = DatanodeUtils.getDataNodeAddress(RdmaConstants.STORAGE_RDMA_INTERFACE, RdmaConstants.STORAGE_RDMA_PORT);
		if (serverAddr == null) {
			logger.info("Configured network interface " + RdmaConstants.STORAGE_RDMA_INTERFACE + " cannot be found..exiting!!!");
			return;
		}
		this.datanodeGroup = new RdmaActiveEndpointGroup<>(-1, false, 1, 1, 1);
		this.datanodeServerEndpoint = datanodeGroup.createServerEndpoint();
		datanodeGroup.init(new RdmaDatanodeEndpointFactory(datanodeGroup, this));
		datanodeServerEndpoint.bind(serverAddr, RdmaConstants.STORAGE_RDMA_BACKLOG);
		
		this.allocatedSize = 0;
		this.fileCount = 0;
		this.dataDirPath = DatanodeUtils.getDatanodeDirectory(RdmaConstants.STORAGE_RDMA_DATA_PATH, serverAddr);
		if (!RdmaConstants.STORAGE_RDMA_PERSISTENT) {
			DatanodeUtils.clean(RdmaConstants.STORAGE_RDMA_DATA_PATH, dataDirPath);
		} 
	}
	
	public void printConf(Logger logger) {
		RdmaConstants.printConf(logger);
	}

	public void close(RdmaEndpoint ep) {
		try {
			allEndpoints.remove(ep.getEndpointId());
			ep.close();
			logger.info("removing endpoint, connCount " + allEndpoints.size());
		} catch (Exception e) {
			logger.info("error closing " + e.getMessage());
		}
	}	
	
	@Override
	public DatanodeResource allocateResource() throws Exception {
		DatanodeResource resource = null;
		
		if (allocatedSize < RdmaConstants.STORAGE_RDMA_STORAGE_LIMIT) {
			//mmap buffer
			int fileId = fileCount++;
			String dataFilePath = dataDirPath + "/" + fileId;
			RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
			if (!RdmaConstants.STORAGE_RDMA_PERSISTENT) {
				dataFile.setLength(RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE);
			}
			FileChannel dataChannel = dataFile.getChannel();
			ByteBuffer dataBuffer = dataChannel.map(MapMode.READ_WRITE, 0, RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE);
			dataFile.close();
			dataChannel.close();

			//register buffer
			allocatedSize += dataBuffer.capacity();
			IbvMr mr = datanodeServerEndpoint.registerMemory(dataBuffer).execute().free().getMr();

			//create resource
			resource = DatanodeResource.createResource(mr.getAddr(), mr.getLength(), mr.getLkey());
		}
		
		return resource;
	}

	@Override
	public void run() {
		try {
			this.isAlive = true;
			logger.info("rdma storage server started, address " + serverAddr + ", persistent " + RdmaConstants.STORAGE_RDMA_PERSISTENT + ", maxWR " + datanodeGroup.getMaxWR() + ", maxSge " + datanodeGroup.getMaxSge() + ", cqSize " + datanodeGroup.getCqSize());
			while (true) {
				RdmaEndpoint clientEndpoint = datanodeServerEndpoint.accept();
				allEndpoints.put(clientEndpoint.getEndpointId(), clientEndpoint);
				logger.info("accepting client connection, conncount " + allEndpoints.size());
			}			
		} catch(Exception e) {
			e.printStackTrace();
		}
		this.isAlive = false;
	}

	@Override
	public InetSocketAddress getAddress() {
		return serverAddr;
	}	
	
	@Override
	public boolean isAlive() {
		return isAlive;
	}
}
