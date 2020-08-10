package com.harana.datagrid.rpc.tcp;

import com.harana.datagrid.narpc.NaRPCServerChannel;
import com.harana.datagrid.narpc.NaRPCService;

import com.harana.datagrid.rpc.RpcErrors;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpRpcDispatcher implements NaRPCService<TcpNameNodeRequest, TcpNameNodeResponse> {
	public static final Logger logger = LogManager.getLogger();
	private RpcNameNodeService service;
	
	public TcpRpcDispatcher(RpcNameNodeService service) {
		this.service = service;
	}

	@Override
	public TcpNameNodeRequest createRequest() {
		return new TcpNameNodeRequest();
	}

	@Override
	public TcpNameNodeResponse processRequest(TcpNameNodeRequest request) {
		TcpNameNodeResponse response = new TcpNameNodeResponse();
		short error = RpcErrors.ERR_OK;
		try {
			short type = RpcProtocol.responseTypes[request.getCmd()];
			response.setType(type);
			switch(request.getCmd()) {
			case RpcProtocol.CMD_CREATE_FILE:
				error = service.createFile(request.createFile(), response.createFile(), response);
				break;			
			case RpcProtocol.CMD_GET_FILE:
				error = service.getFile(request.getFile(), response.getFile(), response);
				break;
			case RpcProtocol.CMD_SET_FILE:
				error = service.setFile(request.setFile(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_REMOVE_FILE:
				error = service.removeFile(request.removeFile(), response.removeFile(), response);
				break;				
			case RpcProtocol.CMD_RENAME_FILE:
				error = service.renameFile(request.renameFile(), response.renameFile(), response);
				break;		
			case RpcProtocol.CMD_GET_BLOCK:
				error = service.getBlock(request.getBlock(), response.getBlock(), response);
				break;
			case RpcProtocol.CMD_GET_LOCATION:
				error = service.getLocation(request.getLocation(), response.getLocation(), response);
				break;				
			case RpcProtocol.CMD_SET_BLOCK:
				error = service.setBlock(request.setBlock(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_GET_DATANODE:
				error = service.getDataNode(request.getDataNode(), response.getDataNode(), response);
				break;					
			case RpcProtocol.CMD_DUMP_NAMENODE:
				error = service.dump(request.dumpNameNode(), response.getVoid(), response);
				break;			
			case RpcProtocol.CMD_PING_NAMENODE:
				error = service.ping(request.pingNameNode(), response.pingNameNode(), response);
				break;
			default:
				error = RpcErrors.ERR_INVALID_RPC_CMD;
				logger.info("Rpc command not valid, opcode " + request.getCmd());
			}
		} catch(Exception e){
			error = RpcErrors.ERR_UNKNOWN;
			logger.info(RpcErrors.messages[RpcErrors.ERR_UNKNOWN] + e.getMessage());
			e.printStackTrace();
		}
		
		try {
			response.setError(error);
		} catch(Exception e){
			logger.info("ERROR: RPC failed, messagesSend ");
			e.printStackTrace();
		}
		
		return response;		
	}

	public void removeEndpoint(NaRPCServerChannel channel){
	}

	public void addEndpoint(NaRPCServerChannel channel){
	}
}
