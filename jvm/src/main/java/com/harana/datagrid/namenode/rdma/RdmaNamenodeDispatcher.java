package com.harana.datagrid.namenode.rdma;

import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException;

import com.harana.datagrid.client.namenode.NamenodeErrors;
import com.harana.datagrid.namenode.NamenodeService;
import com.harana.datagrid.namenode.NamenodeState;
import com.harana.datagrid.namenode.NamenodeProtocol;
import com.harana.datagrid.namenode.NamenodeRequest;
import com.harana.datagrid.namenode.NamenodeResponse;
import com.harana.datagrid.rpc.darpc.DaRPCServerEndpoint;
import com.harana.datagrid.rpc.darpc.DaRPCServerEvent;
import com.harana.datagrid.rpc.darpc.DaRPCService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaNamenodeDispatcher extends RdmaNamenodeProtocol implements DaRPCService<RdmaNamenodeRequest, RdmaNamenodeResponse> {
	private static final Logger logger = LogManager.getLogger();
	
	private final NamenodeService service;
	
	private final AtomicLong totalOps;
	private final AtomicLong createOps;
	private final AtomicLong lookupOps;
	private final AtomicLong setOps;
	private final AtomicLong removeOps;
	private final AtomicLong renameOps;
	private final AtomicLong getOps;
	private final AtomicLong locationOps;
	private final AtomicLong errorOps;
	
	public RdmaNamenodeDispatcher(NamenodeService service) {
		this.service = service;
		
		this.totalOps = new AtomicLong(0);
		this.createOps = new AtomicLong(0);
		this.lookupOps = new AtomicLong(0);
		this.setOps = new AtomicLong(0);
		this.removeOps = new AtomicLong(0);
		this.renameOps = new AtomicLong(0);
		this.getOps = new AtomicLong(0);
		this.locationOps = new AtomicLong(0);
		this.errorOps = new AtomicLong(0);
	}
	
	public void processServerEvent(DaRPCServerEvent<RdmaNamenodeRequest, RdmaNamenodeResponse> event) {
		RdmaNamenodeRequest request = event.getReceiveMessage();
		RdmaNamenodeResponse response = event.getSendMessage();
		short error;
		try {
			response.setType(NamenodeProtocol.responseTypes[request.getCmd()]);
			response.setError((short) 0);
			switch(request.getCmd()) {
			case NamenodeProtocol.CMD_CREATE_FILE:
				this.totalOps.incrementAndGet();
				this.createOps.incrementAndGet();
				error = service.createFile(request.createFile(), response.createFile(), response);
				break;			
			case NamenodeProtocol.CMD_GET_FILE:
				this.totalOps.incrementAndGet();
				this.lookupOps.incrementAndGet();
				error = service.getFile(request.getFile(), response.getFile(), response);
				break;
			case NamenodeProtocol.CMD_SET_FILE:
				this.totalOps.incrementAndGet();
				this.setOps.incrementAndGet();
				error = service.setFile(request.setFile(), response.getVoid(), response);
				break;
			case NamenodeProtocol.CMD_REMOVE_FILE:
				this.totalOps.incrementAndGet();
				this.removeOps.incrementAndGet();
				error = service.removeFile(request.removeFile(), response.delFile(), response);
				break;				
			case NamenodeProtocol.CMD_RENAME_FILE:
				this.totalOps.incrementAndGet();
				this.renameOps.incrementAndGet();
				error = service.renameFile(request.renameFile(), response.getRename(), response);
				break;		
			case NamenodeProtocol.CMD_GET_BLOCK:
				this.totalOps.incrementAndGet();
				this.getOps.incrementAndGet();
				error = service.getBlock(request.getBlock(), response.getBlock(), response);
				break;
			case NamenodeProtocol.CMD_GET_LOCATION:
				this.totalOps.incrementAndGet();
				this.locationOps.incrementAndGet();
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
				error = this.stats(request.pingNameNode(), response.pingNameNode(), response);
				error = service.ping(request.pingNameNode(), response.pingNameNode(), response);
				break;
			default:
				error = NamenodeErrors.ERR_INVALID_RPC_CMD;
				logger.info("Rpc command not valid, opcode " + request.getCmd());
			}
		} catch(Exception e) {
			error = NamenodeErrors.ERR_UNKNOWN;
			this.errorOps.incrementAndGet();
			logger.info(NamenodeErrors.messages[NamenodeErrors.ERR_UNKNOWN] + e.getMessage());
			e.printStackTrace();
		}
		
		try {
			response.setError(error);
			event.triggerResponse();
		} catch(Exception e) {
			logger.info("ERROR: RPC failed, messagesSend ");
			e.printStackTrace();
		}
	}
	
	public short stats(NamenodeRequest.PingNameNode request, NamenodeResponse.PingNameNode response, NamenodeState errorState) throws Exception {
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_PING_NAMENODE, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		logger.info("totalOps " + totalOps.get());
		logger.info("errorOps " + errorOps.get());
		logger.info("createOps " + createOps.get());
		logger.info("lookupOps " + lookupOps.get());
		logger.info("setOps " + setOps.get());
		logger.info("removeOps " + removeOps.get());
		logger.info("renameOps " + renameOps.get());
		logger.info("getOps " + getOps.get());
		logger.info("locationOps " + locationOps.get());
		
		return NamenodeErrors.ERR_OK;
	}	
	
	@Override
	public void open(DaRPCServerEndpoint<RdmaNamenodeRequest, RdmaNamenodeResponse> endpoint) {
		try {
			logger.info("RPC connection, qpnum " + endpoint.getQp().getQp_num());
		} catch(IOException e) {
			logger.info("RPC connection, cannot get qpnum, because QP is not open.\n");
		}
	}	

	@Override
	public void close(DaRPCServerEndpoint<RdmaNamenodeRequest, RdmaNamenodeResponse> endpoint) {
		try {
			logger.info("disconnecting RPC connection, qpnum " + endpoint.getQp().getQp_num());
			endpoint.close();
		} catch(Exception e) {
		}
	}	
}