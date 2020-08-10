package com.harana.datagrid.datanode.tcp;

import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

import com.harana.datagrid.conf.Configuration;
import com.harana.datagrid.conf.Constants;
import com.harana.datagrid.rpc.narpc.NaRPCServerChannel;
import com.harana.datagrid.rpc.narpc.NaRPCServerEndpoint;
import com.harana.datagrid.rpc.narpc.NaRPCServerGroup;
import com.harana.datagrid.rpc.narpc.NaRPCService;
import com.harana.datagrid.datanode.DatanodeResource;
import com.harana.datagrid.datanode.DatanodeServer;
import com.harana.datagrid.datanode.DatanodeUtils;
import com.harana.datagrid.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpDatanodeServer implements Runnable, DatanodeServer, NaRPCService<TcpDatanodeRequest, TcpDatanodeResponse> {
	private static final Logger logger = LogManager.getLogger();
	
	private NaRPCServerGroup<TcpDatanodeRequest, TcpDatanodeResponse> serverGroup;
	private NaRPCServerEndpoint<TcpDatanodeRequest, TcpDatanodeResponse> serverEndpoint;
	private InetSocketAddress address;
	private boolean alive;
	private long regions;
	private long keys;
	private ConcurrentHashMap<Integer, ByteBuffer> dataBuffers;
	private String dataDirPath;
	
	@Override
	public void init(Configuration conf, String[] args) throws Exception {
		TcpDatanodeConstants.init(conf, args);
		
		this.serverGroup = new NaRPCServerGroup<>(this, TcpDatanodeConstants.STORAGE_TCP_QUEUE_DEPTH, (int) Constants.BLOCK_SIZE * 2, false, TcpDatanodeConstants.STORAGE_TCP_CORES);
		this.serverEndpoint = serverGroup.createServerEndpoint();
		this.address = DatanodeUtils.getDataNodeAddress(TcpDatanodeConstants.STORAGE_TCP_INTERFACE, TcpDatanodeConstants.STORAGE_TCP_PORT);
		serverEndpoint.bind(address);
		this.alive = false;
		this.regions = TcpDatanodeConstants.STORAGE_TCP_STORAGE_LIMIT/ TcpDatanodeConstants.STORAGE_TCP_ALLOCATION_SIZE;
		this.keys = 0;
		this.dataBuffers = new ConcurrentHashMap<>();
		this.dataDirPath = DatanodeUtils.getDatanodeDirectory(TcpDatanodeConstants.STORAGE_TCP_DATA_PATH, address);
		DatanodeUtils.clean(TcpDatanodeConstants.STORAGE_TCP_DATA_PATH, dataDirPath);
	}

	@Override
	public void printConf(Logger logger) {
		TcpDatanodeConstants.printConf(logger);
	}

	@Override
	public DatanodeResource allocateResource() throws Exception {
		DatanodeResource resource = null;
		if (keys < regions) {
			int fileId = (int) keys++;
			String dataFilePath = Paths.get(dataDirPath, Integer.toString(fileId)).toString();
			RandomAccessFile dataFile = new RandomAccessFile(dataFilePath, "rw");
			FileChannel dataChannel = dataFile.getChannel();
			ByteBuffer buffer = dataChannel.map(MapMode.READ_WRITE, 0, TcpDatanodeConstants.STORAGE_TCP_ALLOCATION_SIZE);
			dataBuffers.put(fileId, buffer);
			dataFile.close();
			dataChannel.close();			
			long address = Utils.getAddress(buffer);
			resource = DatanodeResource.createResource(address, buffer.capacity(), fileId);
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
			while (true) {
				NaRPCServerChannel endpoint = serverEndpoint.accept();
				logger.info("new connection " + endpoint.address());
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public TcpDatanodeRequest createRequest() {
		return new TcpDatanodeRequest();
	}

	@Override
	public TcpDatanodeResponse processRequest(TcpDatanodeRequest request) {
		if (request.type() == TcpDatanodeProtocol.REQ_WRITE) {
			TcpDatanodeRequest.WriteRequest writeRequest = request.getWriteRequest();
			ByteBuffer buffer = dataBuffers.get(writeRequest.getKey()).duplicate();
			long offset = writeRequest.getAddress() - Utils.getAddress(buffer);
//			logger.info("processing write request, key " + writeRequest.getKey() + ", address " + writeRequest.getAddress() + ", length " + writeRequest.length() + ", remaining " + writeRequest.getBuffer().remaining() + ", offset " + offset);
			buffer.clear().position((int) offset);
			buffer.put(writeRequest.getBuffer());
			TcpDatanodeResponse.WriteResponse writeResponse = new TcpDatanodeResponse.WriteResponse(writeRequest.length());
			return new TcpDatanodeResponse(writeResponse);
		} else if (request.type() == TcpDatanodeProtocol.REQ_READ) {
			TcpDatanodeRequest.ReadRequest readRequest = request.getReadRequest();
			ByteBuffer buffer = dataBuffers.get(readRequest.getKey()).duplicate();
			long offset = readRequest.getAddress() - Utils.getAddress(buffer);
//			logger.info("processing read request, address " + readRequest.getAddress() + ", length " + readRequest.length() + ", offset " + offset);
			long limit = offset + readRequest.length();
			buffer.clear().position((int) offset).limit((int) limit);
			TcpDatanodeResponse.ReadResponse readResponse = new TcpDatanodeResponse.ReadResponse(buffer);
			return new TcpDatanodeResponse(readResponse);
		} else {
			logger.info("processing unknown request");
			return new TcpDatanodeResponse(TcpDatanodeProtocol.RET_RPC_UNKNOWN);
		}
	}

	@Override
	public void addEndpoint(NaRPCServerChannel channel) {
	}

	@Override
	public void removeEndpoint(NaRPCServerChannel channel) {
	}
}