package com.harana.datagrid.namenode.rdma;

import java.io.IOException;

import com.harana.datagrid.DatagridDataType;
import com.harana.datagrid.client.namenode.NamenodeConnection;
import com.harana.datagrid.client.namenode.NamenodeFuture;
import com.harana.datagrid.client.namenode.NamenodeVoid;
import com.harana.datagrid.client.namenode.responses.*;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.metadata.FileInfo;
import com.harana.datagrid.metadata.FileName;
import com.harana.datagrid.namenode.NamenodeProtocol;
import com.harana.datagrid.namenode.NamenodeRequest;
import com.harana.datagrid.namenode.NamenodeResponse;
import com.harana.datagrid.rpc.darpc.DaRPCClientEndpoint;
import com.harana.datagrid.rpc.darpc.DaRPCFuture;
import com.harana.datagrid.rpc.darpc.DaRPCStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaNamenodeConnection implements NamenodeConnection {
	private static final Logger logger = LogManager.getLogger();

	private final DaRPCStream<RdmaNamenodeRequest, RdmaNamenodeResponse> stream;
	private DaRPCClientEndpoint<RdmaNamenodeRequest, RdmaNamenodeResponse> rpcEndpoint;

	public RdmaNamenodeConnection(DaRPCClientEndpoint<RdmaNamenodeRequest, RdmaNamenodeResponse> endpoint) throws IOException {
		this.rpcEndpoint = endpoint;
		this.stream = endpoint.createStream();
	}	
	
	@Override
	public NamenodeFuture<CreateFileResponse> createFile(FileName filename, DatagridDataType type, int storageClass, int locationClass, boolean enumerable) throws IOException {
		if (DatagridConstants.DEBUG) {
			logger.debug("RPC: createFile, fileType " + type + ", storageClass " + storageClass + ", locationClass " + locationClass);
		}
		
		NamenodeRequest.CreateFile createFileReq = new NamenodeRequest.CreateFile(filename, type, storageClass, locationClass, enumerable);
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(createFileReq);
		request.setCommand(NamenodeProtocol.CMD_CREATE_FILE);
		
		NamenodeResponse.CreateFile fileRes = new NamenodeResponse.CreateFile();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(fileRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);
		return new RdmaNamenodeFuture<>(future, fileRes);
	}
	
	@Override
	public NamenodeFuture<GetFileResponse> getFile(FileName filename, boolean writeable) throws IOException {
		if (DatagridConstants.DEBUG) {
			logger.debug("RPC: getFile, writeable " + writeable);
		}
		
		NamenodeRequest.GetFile getFileReq = new NamenodeRequest.GetFile(filename, writeable);
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(getFileReq);
		request.setCommand(NamenodeProtocol.CMD_GET_FILE);

		NamenodeResponse.GetFile fileRes = new NamenodeResponse.GetFile();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(fileRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);
		return new RdmaNamenodeFuture<>(future, fileRes);
	}
	
	@Override
	public RdmaNamenodeFuture<NamenodeVoid> setFile(FileInfo fileInfo, boolean close) throws IOException {
		if (DatagridConstants.DEBUG) {
			logger.debug("RPC: setFile, id " + fileInfo.getFd() + ", close " + close);
		}
		
		NamenodeRequest.SetFile setFileReq = new NamenodeRequest.SetFile(fileInfo, close);
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(setFileReq);
		request.setCommand(NamenodeProtocol.CMD_SET_FILE);
		
		NamenodeResponse.Void voidRes = new NamenodeResponse.Void();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(voidRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);
		return new RdmaNamenodeFuture<>(future, voidRes);
	}
	
	@Override
	public RdmaNamenodeFuture<DeleteFileResponse> removeFile(FileName filename, boolean recursive) throws IOException {
		if (DatagridConstants.DEBUG) {
			logger.debug("RPC: removeFile");
		}
		
		NamenodeRequest.RemoveFile removeReq = new NamenodeRequest.RemoveFile(filename, recursive);
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(removeReq);
		request.setCommand(NamenodeProtocol.CMD_REMOVE_FILE);
		
		NamenodeResponse.DeleteFile fileRes = new NamenodeResponse.DeleteFile();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(fileRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);
		return new RdmaNamenodeFuture<>(future, fileRes);
	}
	
	@Override
	public RdmaNamenodeFuture<RenameFileResponse> renameFile(FileName srcHash, FileName dstHash) throws IOException {
		if (DatagridConstants.DEBUG) {
			logger.debug("RPC: renameFile");
		}
		
		NamenodeRequest.RenameFile renameReq = new NamenodeRequest.RenameFile(srcHash, dstHash);
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(renameReq);
		request.setCommand(NamenodeProtocol.CMD_RENAME_FILE);
		
		NamenodeResponse.RenameFile renameRes = new NamenodeResponse.RenameFile();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(renameRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);
		return new RdmaNamenodeFuture<>(future, renameRes);
	}
	
	@Override
	public RdmaNamenodeFuture<GetBlockResponse> getBlock(long fd, long token, long position, long capacity) throws IOException {
		if (DatagridConstants.DEBUG) {
			logger.debug("RPC: getBlock, fd " + fd + ", token " + token + ", position " + position + ", capacity " + capacity);
		}
		
		NamenodeRequest.GetBlock getBlockReq = new NamenodeRequest.GetBlock(fd, token, position, capacity);
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(getBlockReq);
		request.setCommand(NamenodeProtocol.CMD_GET_BLOCK);
		
		NamenodeResponse.GetBlock getBlockRes = new NamenodeResponse.GetBlock();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(getBlockRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);
		return new RdmaNamenodeFuture<>(future, getBlockRes);
	}
	
	@Override
	public RdmaNamenodeFuture<GetLocationResponse> getLocation(FileName fileName, long position) throws IOException {
		if (DatagridConstants.DEBUG) {
			logger.debug("RPC: getLocation, position " + position);
		}		
		
		NamenodeRequest.GetLocation getLocationReq = new NamenodeRequest.GetLocation(fileName, position);
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(getLocationReq);
		request.setCommand(NamenodeProtocol.CMD_GET_LOCATION);

		NamenodeResponse.GetLocation getLocationRes = new NamenodeResponse.GetLocation();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(getLocationRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);
		return new RdmaNamenodeFuture<>(future, getLocationRes);
	}	
	
	@Override
	public RdmaNamenodeFuture<NamenodeVoid> setBlock(BlockInfo blockInfo) throws Exception {
		if (DatagridConstants.DEBUG) {
			logger.debug("RPC: setBlock, ");
		}		
		
		NamenodeRequest.SetBlock setBlockReq = new NamenodeRequest.SetBlock(blockInfo);
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(setBlockReq);
		request.setCommand(NamenodeProtocol.CMD_SET_BLOCK);
		
		NamenodeResponse.Void voidRes = new NamenodeResponse.Void();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(voidRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);
		return new RdmaNamenodeFuture<>(future, voidRes);
	}
	
	@Override
	public RdmaNamenodeFuture<GetDataNodeResponse> getDataNode(DatanodeInfo dnInfo) throws Exception {
		NamenodeRequest.GetDataNode getDataNodeReq = new NamenodeRequest.GetDataNode(dnInfo);
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(getDataNodeReq);
		request.setCommand(NamenodeProtocol.CMD_GET_DATANODE);
		
		NamenodeResponse.GetDataNode getDataNodeRes = new NamenodeResponse.GetDataNode();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(getDataNodeRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);

		return new RdmaNamenodeFuture<>(future, getDataNodeRes);
	}	
	
	@Override
	public RdmaNamenodeFuture<NamenodeVoid> dumpNameNode() throws Exception {

		NamenodeRequest.DumpNameNode dumpNameNodeReq = new NamenodeRequest.DumpNameNode();
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(dumpNameNodeReq);
		request.setCommand(NamenodeProtocol.CMD_DUMP_NAMENODE);

		NamenodeResponse.Void voidRes = new NamenodeResponse.Void();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(voidRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);

		return new RdmaNamenodeFuture<>(future, voidRes);
	}	
	
	@Override
	public RdmaNamenodeFuture<PingResponse> pingNameNode() throws Exception {
		
		NamenodeRequest.PingNameNode pingReq = new NamenodeRequest.PingNameNode();
		RdmaNamenodeRequest request = new RdmaNamenodeRequest(pingReq);
		request.setCommand(NamenodeProtocol.CMD_PING_NAMENODE);

		NamenodeResponse.PingNameNode pingRes = new NamenodeResponse.PingNameNode();
		RdmaNamenodeResponse response = new RdmaNamenodeResponse(pingRes);
		
		DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future = issueRPC(request, response);
		return new RdmaNamenodeFuture<>(future, pingRes);
	}
	
	@Override
	public void close() throws Exception {
		if (rpcEndpoint != null) {
			rpcEndpoint.close();
			rpcEndpoint = null;
		}		
	}

	private DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> issueRPC(RdmaNamenodeRequest request, RdmaNamenodeResponse response) throws IOException{
		try {
			return stream.request(request, response, false);
		} catch(IOException e) {
			logger.info("ERROR: RPC failed, messagesSend " + rpcEndpoint.getMessagesSent() + ", messagesReceived " + rpcEndpoint.getMessagesReceived() + ", isConnected " + rpcEndpoint.isConnected() + ", qpNum " + rpcEndpoint.getQp().getQp_num());
			throw e;
		}
	}

	@Override
	public String toString() {
		try {
			return rpcEndpoint.getDstAddr().toString();
		} catch(Exception e) {
			return "Unknown";
		}
	}
}