package com.harana.datagrid.namenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.rpc.RpcErrors;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogService {
	public static final Logger logger = LogManager.getLogger();
	
	private ConcurrentHashMap<Long, Long> tokens;
	private FileOutputStream outStream;
	private FileChannel outChannel;
	private ByteBuffer header;
	private ByteBuffer payload;
	
	public LogService() throws IOException {
		File file = new File(CrailConstants.NAMENODE_LOG);
		if (!file.exists()){
			file.createNewFile();
		}
		outStream = new FileOutputStream(CrailConstants.NAMENODE_LOG, true);
		outChannel = outStream.getChannel();
		header = ByteBuffer.allocate(4);
		payload = ByteBuffer.allocate(512);
		tokens = new ConcurrentHashMap<>();
	}
	
	public void writeRecord(LogRecord record) throws IOException{
		payload.clear();
		record.write(payload);
		payload.flip();
		
		header.clear();
		header.putInt(payload.remaining());
		header.flip();
		
		outChannel.write(header);
		while(payload.hasRemaining()){
			outChannel.write(payload);
		}
	}
	
	public void replay(RpcNameNodeService service) throws Exception {
		File file = new File(CrailConstants.NAMENODE_LOG);
		if (!file.exists()){
			return;
		}		
		
		FileInputStream inStream = new FileInputStream(CrailConstants.NAMENODE_LOG);
		FileChannel inChannel = inStream.getChannel();
		LogRecord record = new LogRecord();
		LogResponse response = new LogResponse();
		
		header.clear();
		int ret = inChannel.read(header);
		while(ret > 0){
			header.flip();
			int size = header.getInt();
			
			payload.clear().limit(size);
			while(payload.hasRemaining()){
				inChannel.read(payload);
			}
			payload.flip();
			record.update(payload);
			processServerEvent(service, record, response);
			
			header.clear();
			ret = inChannel.read(header);
		}
		
		inChannel.close();
		inStream.close();
	}
	
	public void close() throws IOException{
		outChannel.close();
		outStream.close();
	}
	
	private void processServerEvent(RpcNameNodeService service, LogRecord record, LogResponse response) {
		short error = RpcErrors.ERR_OK;
		try {
			switch(record.getCmd()) {
			case RpcProtocol.CMD_CREATE_FILE:
				error = service.createFile(record.createFile(), response.createFile(), response);
				long fd = response.createFile().getFile().getFd();
				long token = response.createFile().getFile().getToken();
				tokens.put(response.createFile().getFile().getFd(), response.createFile().getFile().getToken());
				tokens.put(response.createFile().getParent().getFd(), response.createFile().getParent().getToken());
				break;			
			case RpcProtocol.CMD_SET_FILE:
				record.setFile().getFileInfo().setToken(tokens.get(record.setFile().getFileInfo().getFd()));
				error = service.setFile(record.setFile(), response.getVoid(), response);
				break;
			case RpcProtocol.CMD_REMOVE_FILE:
				error = service.removeFile(record.removeFile(), response.delFile(), response);
				break;				
			case RpcProtocol.CMD_RENAME_FILE:
				error = service.renameFile(record.renameFile(), response.getRename(), response);
				break;		
			case RpcProtocol.CMD_GET_BLOCK:
				record.getBlock().setToken(tokens.get(record.getBlock().getFd()));
				error = service.getBlock(record.getBlock(), response.getBlock(), response);
				break;
			case RpcProtocol.CMD_SET_BLOCK:
				error = service.setBlock(record.setBlock(), response.getVoid(), response);
				break;
			default:
				error = RpcErrors.ERR_INVALID_RPC_CMD;
				logger.info("Rpc command not valid, opcode " + record.getCmd());
			}
		} catch(Exception e){
			error = RpcErrors.ERR_UNKNOWN;
			logger.info(RpcErrors.messages[RpcErrors.ERR_UNKNOWN] + e.getMessage());
			e.printStackTrace();
		}
	}	
}
