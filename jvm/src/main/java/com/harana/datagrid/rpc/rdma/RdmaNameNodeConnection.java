package com.harana.datagrid.rpc.rdma;

import java.io.IOException;

import com.harana.datagrid.CrailNodeType;
import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.metadata.FileInfo;
import com.harana.datagrid.metadata.FileName;
import com.harana.datagrid.rpc.RpcConnection;
import com.harana.datagrid.rpc.RpcCreateFile;
import com.harana.datagrid.rpc.RpcDeleteFile;
import com.harana.datagrid.rpc.RpcFuture;
import com.harana.datagrid.rpc.RpcGetBlock;
import com.harana.datagrid.rpc.RpcGetDataNode;
import com.harana.datagrid.rpc.RpcGetFile;
import com.harana.datagrid.rpc.RpcGetLocation;
import com.harana.datagrid.rpc.RpcPing;
import com.harana.datagrid.rpc.RpcProtocol;
import com.harana.datagrid.rpc.RpcRenameFile;
import com.harana.datagrid.rpc.RpcRequestMessage;
import com.harana.datagrid.rpc.RpcResponseMessage;
import com.harana.datagrid.rpc.RpcVoid;
import com.harana.datagrid.darpc.DaRPCClientEndpoint;
import com.harana.datagrid.darpc.DaRPCFuture;
import com.harana.datagrid.darpc.DaRPCStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaNameNodeConnection implements RpcConnection {
	private static final Logger logger = LogManager.getLogger();
	
	private DaRPCClientEndpoint<RdmaNameNodeRequest, RdmaNameNodeResponse> rpcEndpoint;
	private DaRPCStream<RdmaNameNodeRequest, RdmaNameNodeResponse> stream;
	
	public RdmaNameNodeConnection(DaRPCClientEndpoint<RdmaNameNodeRequest, RdmaNameNodeResponse> endpoint) throws IOException {
		this.rpcEndpoint = endpoint;
		this.stream = endpoint.createStream();
	}	
	
	@Override
	public RpcFuture<RpcCreateFile> createFile(FileName filename, CrailNodeType type, int storageClass, int locationClass, boolean enumerable) throws IOException {
		if (CrailConstants.DEBUG){
			logger.debug("RPC: createFile, fileType " + type + ", storageClass " + storageClass + ", locationClass " + locationClass);
		}
		
		RpcRequestMessage.CreateFileReq createFileReq = new RpcRequestMessage.CreateFileReq(filename, type, storageClass, locationClass, enumerable);
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(createFileReq);
		request.setCommand(RpcProtocol.CMD_CREATE_FILE);
		
		RpcResponseMessage.CreateFileRes fileRes = new RpcResponseMessage.CreateFileRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(fileRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);

		return new RdmaNameNodeFuture<>(future, fileRes);
	}
	
	@Override
	public RpcFuture<RpcGetFile> getFile(FileName filename, boolean writeable) throws IOException {
		if (CrailConstants.DEBUG){
			logger.debug("RPC: getFile, writeable " + writeable);
		}
		
		RpcRequestMessage.GetFileReq getFileReq = new RpcRequestMessage.GetFileReq(filename, writeable);
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(getFileReq);
		request.setCommand(RpcProtocol.CMD_GET_FILE);

		RpcResponseMessage.GetFileRes fileRes = new RpcResponseMessage.GetFileRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(fileRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);

		return new RdmaNameNodeFuture<>(future, fileRes);
	}
	
	@Override
	public RdmaNameNodeFuture<RpcVoid> setFile(FileInfo fileInfo, boolean close) throws IOException {
		if (CrailConstants.DEBUG){
			logger.debug("RPC: setFile, id " + fileInfo.getFd() + ", close " + close);
		}
		
		RpcRequestMessage.SetFileReq setFileReq = new RpcRequestMessage.SetFileReq(fileInfo, close);
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(setFileReq);
		request.setCommand(RpcProtocol.CMD_SET_FILE);
		
		RpcResponseMessage.VoidRes voidRes = new RpcResponseMessage.VoidRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(voidRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);

		return new RdmaNameNodeFuture<>(future, voidRes);
	}
	
	@Override
	public RdmaNameNodeFuture<RpcDeleteFile> removeFile(FileName filename, boolean recursive) throws IOException {
		if (CrailConstants.DEBUG){
			logger.debug("RPC: removeFile");
		}
		
		RpcRequestMessage.RemoveFileReq removeReq = new RpcRequestMessage.RemoveFileReq(filename, recursive);
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(removeReq);
		request.setCommand(RpcProtocol.CMD_REMOVE_FILE);
		
		RpcResponseMessage.DeleteFileRes fileRes = new RpcResponseMessage.DeleteFileRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(fileRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);
		
		RdmaNameNodeFuture<RpcDeleteFile> nameNodeFuture = new RdmaNameNodeFuture<RpcDeleteFile>(future, fileRes);
		
		return nameNodeFuture;			
	}
	
	@Override
	public RdmaNameNodeFuture<RpcRenameFile> renameFile(FileName srcHash, FileName dstHash) throws IOException {
		if (CrailConstants.DEBUG){
			logger.debug("RPC: renameFile");
		}
		
		RpcRequestMessage.RenameFileReq renameReq = new RpcRequestMessage.RenameFileReq(srcHash, dstHash);
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(renameReq);
		request.setCommand(RpcProtocol.CMD_RENAME_FILE);
		
		RpcResponseMessage.RenameRes renameRes = new RpcResponseMessage.RenameRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(renameRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);
		return new RdmaNameNodeFuture<>(future, renameRes);
	}
	
	@Override
	public RdmaNameNodeFuture<RpcGetBlock> getBlock(long fd, long token, long position, long capacity) throws IOException {
		if (CrailConstants.DEBUG){
			logger.debug("RPC: getBlock, fd " + fd + ", token " + token + ", position " + position + ", capacity " + capacity);
		}
		
		RpcRequestMessage.GetBlockReq getBlockReq = new RpcRequestMessage.GetBlockReq(fd, token, position, capacity);
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(getBlockReq);
		request.setCommand(RpcProtocol.CMD_GET_BLOCK);
		
		RpcResponseMessage.GetBlockRes getBlockRes = new RpcResponseMessage.GetBlockRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(getBlockRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);
		return new RdmaNameNodeFuture<>(future, getBlockRes);
	}
	
	@Override
	public RdmaNameNodeFuture<RpcGetLocation> getLocation(FileName fileName, long position) throws IOException {
		if (CrailConstants.DEBUG){
			logger.debug("RPC: getLocation, position " + position);
		}		
		
		RpcRequestMessage.GetLocationReq getLocationReq = new RpcRequestMessage.GetLocationReq(fileName, position);
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(getLocationReq);
		request.setCommand(RpcProtocol.CMD_GET_LOCATION);

		RpcResponseMessage.GetLocationRes getLocationRes = new RpcResponseMessage.GetLocationRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(getLocationRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);
		return new RdmaNameNodeFuture<>(future, getLocationRes);
	}	
	
	@Override
	public RdmaNameNodeFuture<RpcVoid> setBlock(BlockInfo blockInfo) throws Exception {
		if (CrailConstants.DEBUG){
			logger.debug("RPC: setBlock, ");
		}		
		
		RpcRequestMessage.SetBlockReq setBlockReq = new RpcRequestMessage.SetBlockReq(blockInfo);
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(setBlockReq);
		request.setCommand(RpcProtocol.CMD_SET_BLOCK);
		
		RpcResponseMessage.VoidRes voidRes = new RpcResponseMessage.VoidRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(voidRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);
		return new RdmaNameNodeFuture<>(future, voidRes);
	}
	
	@Override
	public RdmaNameNodeFuture<RpcGetDataNode> getDataNode(DataNodeInfo dnInfo) throws Exception {
		RpcRequestMessage.GetDataNodeReq getDataNodeReq = new RpcRequestMessage.GetDataNodeReq(dnInfo);
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(getDataNodeReq);
		request.setCommand(RpcProtocol.CMD_GET_DATANODE);
		
		RpcResponseMessage.GetDataNodeRes getDataNodeRes = new RpcResponseMessage.GetDataNodeRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(getDataNodeRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);

		return new RdmaNameNodeFuture<>(future, getDataNodeRes);
	}	
	
	@Override
	public RdmaNameNodeFuture<RpcVoid> dumpNameNode() throws Exception {

		RpcRequestMessage.DumpNameNodeReq dumpNameNodeReq = new RpcRequestMessage.DumpNameNodeReq();
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(dumpNameNodeReq);
		request.setCommand(RpcProtocol.CMD_DUMP_NAMENODE);

		RpcResponseMessage.VoidRes voidRes = new RpcResponseMessage.VoidRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(voidRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);

		return new RdmaNameNodeFuture<>(future, voidRes);
	}	
	
	@Override
	public RdmaNameNodeFuture<RpcPing> pingNameNode() throws Exception {
		
		RpcRequestMessage.PingNameNodeReq pingReq = new RpcRequestMessage.PingNameNodeReq();
		RdmaNameNodeRequest request = new RdmaNameNodeRequest(pingReq);
		request.setCommand(RpcProtocol.CMD_PING_NAMENODE);

		RpcResponseMessage.PingNameNodeRes pingRes = new RpcResponseMessage.PingNameNodeRes();
		RdmaNameNodeResponse response = new RdmaNameNodeResponse(pingRes);
		
		DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future = issueRPC(request, response);
		return new RdmaNameNodeFuture<>(future, pingRes);
	}
	
	@Override
	public void close() throws Exception {
		if (rpcEndpoint != null){
			rpcEndpoint.close();
			rpcEndpoint = null;
		}		
	}

	private DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> issueRPC(RdmaNameNodeRequest request, RdmaNameNodeResponse response) throws IOException{
		try {
			return stream.request(request, response, false);
		} catch(IOException e){
			logger.info("ERROR: RPC failed, messagesSend " + rpcEndpoint.getMessagesSent() + ", messagesReceived " + rpcEndpoint.getMessagesReceived() + ", isConnected " + rpcEndpoint.isConnected() + ", qpNum " + rpcEndpoint.getQp().getQp_num());
			throw e;
		}
	}

	@Override
	public String toString() {
		try {
			return rpcEndpoint.getDstAddr().toString();
		} catch(Exception e){
			return "Unknown";
		}
	}
}
