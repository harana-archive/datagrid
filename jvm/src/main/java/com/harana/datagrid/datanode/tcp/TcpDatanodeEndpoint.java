package com.harana.datagrid.datanode.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.harana.datagrid.DatagridBuffer;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.client.datanode.DatanodeFuture;
import com.harana.datagrid.rpc.narpc.NaRPCEndpoint;
import com.harana.datagrid.rpc.narpc.NaRPCFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpDatanodeEndpoint implements DatanodeEndpoint {
	private static final Logger logger = LogManager.getLogger();
	private final NaRPCEndpoint<TcpDatanodeRequest, TcpDatanodeResponse> endpoint;
	
	public TcpDatanodeEndpoint(NaRPCEndpoint<TcpDatanodeRequest, TcpDatanodeResponse> endpoint) {
		this.endpoint = endpoint;
	}

	public void connect(InetSocketAddress address) throws IOException {
		endpoint.connect(address);
	}

	@Override
	public void close() throws IOException {
		endpoint.close();
	}

	@Override
	public boolean isLocal() {
		return false;
	}

	@Override
	public DatanodeFuture read(DatagridBuffer buffer, BlockInfo block, long offset) throws IOException {
		logger.info("TCP read, buffer " + buffer.remaining() + ", block " + block.getLkey() + "/" + block.getAddr() + "/" + block.getLength() + ", offset " + offset);
		TcpDatanodeRequest.ReadRequest readReq = new TcpDatanodeRequest.ReadRequest(block.getLkey(), block.getAddr() + offset, buffer.remaining());
		TcpDatanodeResponse.ReadResponse readResp = new TcpDatanodeResponse.ReadResponse(buffer.getByteBuffer());
		
		TcpDatanodeRequest req = new TcpDatanodeRequest(readReq);
		TcpDatanodeResponse resp = new TcpDatanodeResponse(readResp);
		
		NaRPCFuture<TcpDatanodeRequest, TcpDatanodeResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new TcpDatanodeFuture(narpcFuture, readReq.length());
	}

	@Override
	public DatanodeFuture write(DatagridBuffer buffer, BlockInfo block, long offset) throws IOException {
		logger.info("TCP write, buffer " + buffer.remaining() + ", block " +  block.getLkey() + "/" + block.getAddr() + "/" + block.getLength() + ", offset " + offset);
		TcpDatanodeRequest.WriteRequest writeReq = new TcpDatanodeRequest.WriteRequest(block.getLkey(), block.getAddr() + offset, buffer.remaining(), buffer.getByteBuffer());
		TcpDatanodeResponse.WriteResponse writeResp = new TcpDatanodeResponse.WriteResponse();
		
		TcpDatanodeRequest req = new TcpDatanodeRequest(writeReq);
		TcpDatanodeResponse resp = new TcpDatanodeResponse(writeResp);
		
		NaRPCFuture<TcpDatanodeRequest, TcpDatanodeResponse> narpcFuture = endpoint.issueRequest(req, resp);
		return new TcpDatanodeFuture(narpcFuture, writeReq.length());
	}
}