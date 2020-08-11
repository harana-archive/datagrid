package com.harana.datagrid.namenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

import com.harana.datagrid.client.namenode.NamenodeErrors;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.namenode.metadata.LogRecord;
import com.harana.datagrid.namenode.metadata.LogResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NamenodeLogService {
	public static final Logger logger = LogManager.getLogger();
	
	private final ConcurrentHashMap<Long, Long> tokens;
	private final FileOutputStream outStream;
	private final FileChannel outChannel;
	private final ByteBuffer header;
	private final ByteBuffer payload;
	
	public NamenodeLogService() throws IOException {
		File file = new File(DatagridConstants.NAMENODE_LOG);
		if (!file.exists()) {
			file.createNewFile();
		}
		outStream = new FileOutputStream(DatagridConstants.NAMENODE_LOG, true);
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
		while (payload.hasRemaining()) {
			outChannel.write(payload);
		}
	}
	
	public void replay(NamenodeService service) throws Exception {
		File file = new File(DatagridConstants.NAMENODE_LOG);
		if (!file.exists()) {
			return;
		}		
		
		FileInputStream inStream = new FileInputStream(DatagridConstants.NAMENODE_LOG);
		FileChannel inChannel = inStream.getChannel();
		LogRecord record = new LogRecord();
		LogResponse response = new LogResponse();
		
		header.clear();
		int ret = inChannel.read(header);
		while (ret > 0) {
			header.flip();
			int size = header.getInt();
			
			payload.clear().limit(size);
			while (payload.hasRemaining()) {
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
	
	private void processServerEvent(NamenodeService service, LogRecord record, LogResponse response) {
		short error = NamenodeErrors.ERR_OK;
		try {
			switch(record.getCmd()) {
			case NamenodeProtocol.CMD_CREATE_FILE:
				error = service.createFile(record.createFile(), response.createFile(), response);
				long fd = response.createFile().getFile().getFd();
				long token = response.createFile().getFile().getToken();
				tokens.put(response.createFile().getFile().getFd(), response.createFile().getFile().getToken());
				tokens.put(response.createFile().getParent().getFd(), response.createFile().getParent().getToken());
				break;			
			case NamenodeProtocol.CMD_SET_FILE:
				record.setFile().getFileInfo().setToken(tokens.get(record.setFile().getFileInfo().getFd()));
				error = service.setFile(record.setFile(), response.getVoid(), response);
				break;
			case NamenodeProtocol.CMD_REMOVE_FILE:
				error = service.removeFile(record.removeFile(), response.delFile(), response);
				break;				
			case NamenodeProtocol.CMD_RENAME_FILE:
				error = service.renameFile(record.renameFile(), response.getRename(), response);
				break;		
			case NamenodeProtocol.CMD_GET_BLOCK:
				record.getBlock().setToken(tokens.get(record.getBlock().getFd()));
				error = service.getBlock(record.getBlock(), response.getBlock(), response);
				break;
			case NamenodeProtocol.CMD_SET_BLOCK:
				error = service.setBlock(record.setBlock(), response.getVoid(), response);
				break;
			default:
				error = NamenodeErrors.ERR_INVALID_RPC_CMD;
				logger.info("Rpc command not valid, opcode " + record.getCmd());
			}
		} catch(Exception e) {
			error = NamenodeErrors.ERR_UNKNOWN;
			logger.info(NamenodeErrors.messages[NamenodeErrors.ERR_UNKNOWN] + e.getMessage());
			e.printStackTrace();
		}
	}	
}