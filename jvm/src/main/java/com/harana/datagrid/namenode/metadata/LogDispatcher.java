package com.harana.datagrid.namenode.metadata;

import com.harana.datagrid.namenode.*;
import com.harana.datagrid.namenode.NamenodeRequest.CreateFile;
import com.harana.datagrid.namenode.NamenodeRequest.DumpNameNode;
import com.harana.datagrid.namenode.NamenodeRequest.GetBlock;
import com.harana.datagrid.namenode.NamenodeRequest.GetDataNode;
import com.harana.datagrid.namenode.NamenodeRequest.GetFile;
import com.harana.datagrid.namenode.NamenodeRequest.GetLocation;
import com.harana.datagrid.namenode.NamenodeRequest.PingNameNode;
import com.harana.datagrid.namenode.NamenodeRequest.RemoveFile;
import com.harana.datagrid.namenode.NamenodeRequest.SetBlock;
import com.harana.datagrid.namenode.NamenodeRequest.SetFile;
import com.harana.datagrid.namenode.NamenodeResponse.DeleteFile;
import com.harana.datagrid.namenode.NamenodeResponse.RenameFile;
import com.harana.datagrid.namenode.NamenodeResponse.Void;

public class LogDispatcher implements NamenodeService {
	private NamenodeService service;
	private NamenodeLogService logService;
	
	public LogDispatcher(NamenodeService service) throws Exception{
		this.service = service;
		this.logService = new NamenodeLogService();
		this.logService.replay(service);
	}

	@Override
	public short createFile(CreateFile request, NamenodeResponse.CreateFile response, NamenodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(NamenodeProtocol.CMD_CREATE_FILE);
		logService.writeRecord(record);
		return service.createFile(request, response, errorState);
	}

	@Override
	public short getFile(GetFile request, NamenodeResponse.GetFile response, NamenodeState errorState) throws Exception {
		return service.getFile(request, response, errorState);
	}

	@Override
	public short setFile(SetFile request, Void response, NamenodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(NamenodeProtocol.CMD_SET_FILE);
		logService.writeRecord(record);		
		return service.setFile(request, response, errorState);
	}

	@Override
	public short removeFile(RemoveFile request, DeleteFile response, NamenodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(NamenodeProtocol.CMD_REMOVE_FILE);
		logService.writeRecord(record);		
		return service.removeFile(request, response, errorState);
	}

	@Override
	public short renameFile(NamenodeRequest.RenameFile request, RenameFile response, NamenodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(NamenodeProtocol.CMD_RENAME_FILE);
		logService.writeRecord(record);		
		return service.renameFile(request, response, errorState);
	}

	@Override
	public short getDataNode(GetDataNode request, NamenodeResponse.GetDataNode response, NamenodeState errorState) throws Exception {
		return service.getDataNode(request, response, errorState);
	}

	@Override
	public short setBlock(SetBlock request, Void response, NamenodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(NamenodeProtocol.CMD_SET_BLOCK);
		logService.writeRecord(record);		
		return service.setBlock(request, response, errorState);
	}

	@Override
	public short getBlock(GetBlock request, NamenodeResponse.GetBlock response, NamenodeState errorState) throws Exception {
		LogRecord record = new LogRecord(request);
		record.setCommand(NamenodeProtocol.CMD_GET_BLOCK);
		logService.writeRecord(record);		
		return service.getBlock(request, response, errorState);
	}

	@Override
	public short getLocation(GetLocation request, NamenodeResponse.GetLocation response, NamenodeState errorState) throws Exception {
		return service.getLocation(request, response, errorState);
	}

	@Override
	public short dump(DumpNameNode request, Void response, NamenodeState errorState) throws Exception {
		return service.dump(request, response, errorState);
	}

	@Override
	public short ping(PingNameNode request, NamenodeResponse.PingNameNode response, NamenodeState errorState) throws Exception {
		return service.ping(request, response, errorState);
	}
}