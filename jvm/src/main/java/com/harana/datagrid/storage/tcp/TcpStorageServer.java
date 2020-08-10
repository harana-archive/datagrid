package com.harana.datagrid.storage.tcp;

import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.narpc.NaRPCServerChannel;
import com.harana.datagrid.narpc.NaRPCServerEndpoint;
import com.harana.datagrid.narpc.NaRPCServerGroup;
import com.harana.datagrid.narpc.NaRPCService;
import com.harana.datagrid.storage.StorageResource;
import com.harana.datagrid.storage.StorageServer;
import com.harana.datagrid.storage.StorageUtils;
import com.harana.datagrid.utils.CrailUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpStorageServer implements Runnable, StorageServer, NaRPCService<TcpStorageRequest, TcpStorageResponse> {
	private static final Logger logger = LogManager.getLogger();
	
	private NaRPCServerGroup<TcpStorageRequest, TcpStorageResponse> serverGroup;
	private NaRPCServerEndpoint<TcpStorageRequest, TcpStorageResponse> serverEndpoint;
	private InetSocketAddress address;
	private boolean alive;
	private long regions;
	private long keys;
	private ConcurrentHashMap<Integer, ByteBuffer> dataBuffers;
	private String dataDirPath;
	
	@Override
	public void init(CrailConfiguration conf, String[] args) throws Exception {
		TcpStorageConstants.init(conf, args);
		
		this.serverGroup = new NaRPCServerGroup<>(this, TcpStorageConstants.STORAGE_TCP_QUEUE_DEPTH, (int) CrailConstants.BLOCK_SIZE * 2, false, TcpStorageConstants.STORAGE_TCP_CORES);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		this.address = StorageUtils.getDataNodeAddress(TcpStorageConstants.STORAGE_TCP_INTERFACE, TcpStorageConstants.STORAGE_TCP_PORT);
		serverEndpoint.bind(address);
		this.alive = false;
		this.regions = TcpStorageConstants.STORAGE_TCP_STORAGE_LIMIT/TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE;
		this.keys = 0;
		this.dataBuffers = new ConcurrentHashMap<>();
		this.dataDirPath = StorageUtils.getDatanodeDirectory(TcpStorageConstants.STORAGE_TCP_DATA_PATH, address);
		StorageUtils.clean(TcpStorageConstants.STORAGE_TCP_DATA_PATH, dataDirPath);
	}

	@Override
	public void printConf(Logger logger) {
		TcpStorageConstants.printConf(logger);
	}

	@Override
	public StorageResource allocateResource() throws Exception {
		StorageResource resource = null;
		if (keys < regions){
			int fileId = (int) keys++;
			String dataFilePath = Paths.get(dataDirPath, Integer.toString(fileId)).toString();
			RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
			FileChannel dataChannel = dataFile.getChannel();
			ByteBuffer buffer = dataChannel.map(MapMode.READ_WRITE, 0, TcpStorageConstants.STORAGE_TCP_ALLOCATION_SIZE);
			dataBuffers.put(fileId, buffer);
			dataFile.close();
			dataChannel.close();			
			long address = CrailUtils.getAddress(buffer);
			resource = StorageResource.createResource(address, buffer.capacity(), fileId);
		}
		return resource;
	}

	@Override
	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public boolean isAlive() {
		return alive;
	}

	@Override
	public void run() {
		try {
			logger.info("running TCP storage server, address " + address);
			this.alive = true;
			while(true){
				NaRPCServerChannel endpoint = serverEndpoint.accept();
				logger.info("new connection " + endpoint.address());
			}
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public TcpStorageRequest createRequest() {
		return new TcpStorageRequest();
	}

	@Override
	public TcpStorageResponse processRequest(TcpStorageRequest request) {
		if (request.type() == TcpStorageProtocol.REQ_WRITE){
			TcpStorageRequest.WriteRequest writeRequest = request.getWriteRequest();
			ByteBuffer buffer = dataBuffers.get(writeRequest.getKey()).duplicate();
			long offset = writeRequest.getAddress() - CrailUtils.getAddress(buffer);
//			logger.info("processing write request, key " + writeRequest.getKey() + ", address " + writeRequest.getAddress() + ", length " + writeRequest.length() + ", remaining " + writeRequest.getBuffer().remaining() + ", offset " + offset);
			buffer.clear().position((int) offset);
			buffer.put(writeRequest.getBuffer());
			TcpStorageResponse.WriteResponse writeResponse = new TcpStorageResponse.WriteResponse(writeRequest.length());
			return new TcpStorageResponse(writeResponse);
		} else if (request.type() == TcpStorageProtocol.REQ_READ){
			TcpStorageRequest.ReadRequest readRequest = request.getReadRequest();
			ByteBuffer buffer = dataBuffers.get(readRequest.getKey()).duplicate();
			long offset = readRequest.getAddress() - CrailUtils.getAddress(buffer);
//			logger.info("processing read request, address " + readRequest.getAddress() + ", length " + readRequest.length() + ", offset " + offset);
			long limit = offset + readRequest.length();
			buffer.clear().position((int) offset).limit((int) limit);
			TcpStorageResponse.ReadResponse readResponse = new TcpStorageResponse.ReadResponse(buffer);
			return new TcpStorageResponse(readResponse);
		} else {
			logger.info("processing unknown request");
			return new TcpStorageResponse(TcpStorageProtocol.RET_RPC_UNKNOWN);
		}
	}

	@Override
	public void addEndpoint(NaRPCServerChannel channel){
	}

	@Override
	public void removeEndpoint(NaRPCServerChannel channel){
	}
}