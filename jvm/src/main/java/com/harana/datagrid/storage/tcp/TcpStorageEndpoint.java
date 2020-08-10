package com.harana.datagrid.storage.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.harana.datagrid.CrailBuffer;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.narpc.NaRPCEndpoint;
import com.harana.datagrid.narpc.NaRPCFuture;
import com.harana.datagrid.storage.StorageEndpoint;
import com.harana.datagrid.storage.StorageFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpStorageEndpoint implements StorageEndpoint {
	private static final Logger logger = LogManager.getLogger();
	private NaRPCEndpoint<TcpStorageRequest, TcpStorageResponse> endpoint;
	
	public TcpStorageEndpoint(NaRPCEndpoint<TcpStorageRequest, TcpStorageResponse> endpoint) {
		this.endpoint = endpoint;
	}

	public void connect(InetSocketAddress address) throws IOException {
		endpoint.connect(address);
	}

	@Override
	public void close() throws IOException, InterruptedException {
		endpoint.close();
	}

	@Override
	public boolean isLocal() {
		return false;
	}

	@Override
	public StorageFuture read(CrailBuffer buffer, BlockInfo block, long offset)
			throws IOException, InterruptedException {
//		logger.info("TCP read, buffer " + buffer.remaining() + ", block " + block.getLkey() + "/" + block.getAddr() + "/" + block.getLength() + ", offset " + offset);
		TcpStorageRequest.ReadRequest readReq = new TcpStorageRequest.ReadRequest(block.getLkey(), block.getAddr() + offset, buffer.remaining());
		TcpStorageResponse.ReadResponse readResp = new TcpStorageResponse.ReadResponse(buffer.getByteBuffer());
		
		TcpStorageRequest req = new TcpStorageRequest(readReq);
		TcpStorageResponse resp = new TcpStorageResponse(readResp);
		
		NaRPCFuture<TcpStorageRequest, TcpStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new TcpStorageFuture(narpcFuture, readReq.length());
	}

	@Override
	public StorageFuture write(CrailBuffer buffer, BlockInfo block, long offset)
			throws IOException, InterruptedException {
//		logger.info("TCP write, buffer " + buffer.remaining() + ", block " +  block.getLkey() + "/" + block.getAddr() + "/" + block.getLength() + ", offset " + offset);
		TcpStorageRequest.WriteRequest writeReq = new TcpStorageRequest.WriteRequest(block.getLkey(), block.getAddr() + offset, buffer.remaining(), buffer.getByteBuffer());
		TcpStorageResponse.WriteResponse writeResp = new TcpStorageResponse.WriteResponse();
		
		TcpStorageRequest req = new TcpStorageRequest(writeReq);
		TcpStorageResponse resp = new TcpStorageResponse(writeResp);
		
		NaRPCFuture<TcpStorageRequest, TcpStorageResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new TcpStorageFuture(narpcFuture, writeReq.length());
	}

}
