package com.harana.datagrid.namenode.tcp;

import com.harana.datagrid.client.namenode.NamenodeConnection;
import com.harana.datagrid.client.namenode.NamenodeFuture;
import com.harana.datagrid.client.namenode.NamenodeVoid;

import com.harana.datagrid.DataType;
import com.harana.datagrid.client.namenode.responses.*;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.metadata.FileInfo;
import com.harana.datagrid.metadata.FileName;
import com.harana.datagrid.namenode.NamenodeProtocol;
import com.harana.datagrid.namenode.NamenodeRequest;
import com.harana.datagrid.namenode.NamenodeResponse;
import com.harana.datagrid.rpc.narpc.NaRPCEndpoint;
import com.harana.datagrid.rpc.narpc.NaRPCFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class TcpNamenodeConnection implements NamenodeConnection {
	static private final Logger logger = LogManager.getLogger();
	private final NaRPCEndpoint<TcpNamenodeRequest, TcpNamenodeResponse> endpoint;

	public TcpNamenodeConnection(NaRPCEndpoint<TcpNamenodeRequest, TcpNamenodeResponse> endpoint) {
		this.endpoint = endpoint;
	}

	public String toString() {
		String address = "";
		try {
			address = endpoint.address();
		} catch (IOException e) {}
		return address;
	}

	public void close() throws IOException {
		this.endpoint.close();
	}

	public NamenodeFuture<CreateFileResponse> createFile(FileName fileName, DataType type, int storageAffinity, int locationAffinity, boolean enumerable) throws IOException {
		NamenodeRequest.CreateFile req = new NamenodeRequest.CreateFile(fileName, type, storageAffinity, locationAffinity, enumerable);
		NamenodeResponse.CreateFile resp = new NamenodeResponse.CreateFile();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_CREATE_FILE);

		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<GetFileResponse> getFile(FileName fileName, boolean b) throws IOException {
		NamenodeRequest.GetFile req = new NamenodeRequest.GetFile(fileName, b);
		NamenodeResponse.GetFile resp = new NamenodeResponse.GetFile();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_GET_FILE);

		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<NamenodeVoid> setFile(FileInfo fileInfo, boolean b) throws IOException {
		NamenodeRequest.SetFile req = new NamenodeRequest.SetFile(fileInfo, b);
		NamenodeResponse.Void resp = new NamenodeResponse.Void();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_SET_FILE);
		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<DeleteFileResponse> removeFile(FileName fileName, boolean b) throws IOException {
		NamenodeRequest.RemoveFile req = new NamenodeRequest.RemoveFile(fileName, b);
		NamenodeResponse.DeleteFile resp = new NamenodeResponse.DeleteFile();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_REMOVE_FILE);

		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<RenameFileResponse> renameFile(FileName fileName, FileName fileName1) throws IOException {
		NamenodeRequest.RenameFile req = new NamenodeRequest.RenameFile(fileName, fileName1);
		NamenodeResponse.RenameFile resp = new NamenodeResponse.RenameFile();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_RENAME_FILE);
		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<GetBlockResponse> getBlock(long fd, long token, long position, long capacity) throws IOException {
		NamenodeRequest.GetBlock req = new NamenodeRequest.GetBlock(fd, token, position, capacity);
		NamenodeResponse.GetBlock resp = new NamenodeResponse.GetBlock();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_GET_BLOCK);
		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<GetLocationResponse> getLocation(FileName fileName, long l) throws IOException {
		NamenodeRequest.GetLocation req = new NamenodeRequest.GetLocation(fileName, l);
		NamenodeResponse.GetLocation resp = new NamenodeResponse.GetLocation();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_GET_LOCATION);
		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<NamenodeVoid> setBlock(BlockInfo blockInfo) throws IOException {
		NamenodeRequest.SetBlock req = new NamenodeRequest.SetBlock(blockInfo);
		NamenodeResponse.Void resp = new NamenodeResponse.Void();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_SET_BLOCK);
		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<GetDataNodeResponse> getDataNode(DatanodeInfo dataNodeInfo) throws IOException {
		NamenodeRequest.GetDataNode req = new NamenodeRequest.GetDataNode(dataNodeInfo);
		NamenodeResponse.GetDataNode resp = new NamenodeResponse.GetDataNode();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_GET_DATANODE);
		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<NamenodeVoid> dumpNameNode() throws IOException {
		NamenodeRequest.DumpNameNode req = new NamenodeRequest.DumpNameNode();
		NamenodeResponse.Void resp = new NamenodeResponse.Void();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_DUMP_NAMENODE);
		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}

	public NamenodeFuture<PingResponse> pingNameNode() throws IOException {
		NamenodeRequest.PingNameNode req = new NamenodeRequest.PingNameNode();
		NamenodeResponse.PingNameNode resp = new NamenodeResponse.PingNameNode();

		TcpNamenodeRequest request = new TcpNamenodeRequest(req);
		TcpNamenodeResponse response = new TcpNamenodeResponse(resp);
		request.setCommand(NamenodeProtocol.CMD_PING_NAMENODE);
		NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future = endpoint.issueRequest(request, response);
		return new TcpFuture<>(future, resp);
	}
}