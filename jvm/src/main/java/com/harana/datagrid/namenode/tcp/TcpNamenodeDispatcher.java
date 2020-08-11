package com.harana.datagrid.namenode.tcp;

import com.harana.datagrid.client.namenode.NamenodeErrors;

import com.harana.datagrid.namenode.NamenodeService;
import com.harana.datagrid.namenode.NamenodeProtocol;
import com.harana.datagrid.rpc.narpc.NaRPCServerChannel;
import com.harana.datagrid.rpc.narpc.NaRPCService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpNamenodeDispatcher implements NaRPCService<TcpNamenodeRequest, TcpNamenodeResponse> {
	public static final Logger logger = LogManager.getLogger();
	private final NamenodeService service;
	
	public TcpNamenodeDispatcher(NamenodeService service) {
		this.service = service;
	}

	@Override
	public TcpNamenodeRequest createRequest() {
		return new TcpNamenodeRequest();
	}

	@Override
	public TcpNamenodeResponse processRequest(TcpNamenodeRequest request) {
		TcpNamenodeResponse response = new TcpNamenodeResponse();
		short error;
		try {
			short type = NamenodeProtocol.responseTypes[request.getCmd()];
			response.setType(type);
			switch(request.getCmd()) {
			case NamenodeProtocol.CMD_CREATE_FILE:
				error = service.createFile(request.createFile(), response.createFile(), response);
				break;			
			case NamenodeProtocol.CMD_GET_FILE:
				error = service.getFile(request.getFile(), response.getFile(), response);
				break;
			case NamenodeProtocol.CMD_SET_FILE:
				error = service.setFile(request.setFile(), response.getVoid(), response);
				break;
			case NamenodeProtocol.CMD_REMOVE_FILE:
				error = service.removeFile(request.removeFile(), response.removeFile(), response);
				break;				
			case NamenodeProtocol.CMD_RENAME_FILE:
				error = service.renameFile(request.renameFile(), response.renameFile(), response);
				break;		
			case NamenodeProtocol.CMD_GET_BLOCK:
				error = service.getBlock(request.getBlock(), response.getBlock(), response);
				break;
			case NamenodeProtocol.CMD_GET_LOCATION:
				error = service.getLocation(request.getLocation(), response.getLocation(), response);
				break;				
			case NamenodeProtocol.CMD_SET_BLOCK:
				error = service.setBlock(request.setBlock(), response.getVoid(), response);
				break;
			case NamenodeProtocol.CMD_GET_DATANODE:
				error = service.getDataNode(request.getDataNode(), response.getDataNode(), response);
				break;					
			case NamenodeProtocol.CMD_DUMP_NAMENODE:
				error = service.dump(request.dumpNameNode(), response.getVoid(), response);
				break;			
			case NamenodeProtocol.CMD_PING_NAMENODE:
				error = service.ping(request.pingNameNode(), response.pingNameNode(), response);
				break;
			default:
				error = NamenodeErrors.ERR_INVALID_RPC_CMD;
				logger.info("Rpc command not valid, opcode " + request.getCmd());
			}
		} catch(Exception e) {
			error = NamenodeErrors.ERR_UNKNOWN;
			logger.info(NamenodeErrors.messages[NamenodeErrors.ERR_UNKNOWN] + e.getMessage());
			e.printStackTrace();
		}
		
		try {
			response.setError(error);
		} catch(Exception e) {
			logger.info("ERROR: RPC failed, messagesSend ");
			e.printStackTrace();
		}
		
		return response;		
	}

	public void removeEndpoint(NaRPCServerChannel channel) {
	}

	public void addEndpoint(NaRPCServerChannel channel) {
	}
}