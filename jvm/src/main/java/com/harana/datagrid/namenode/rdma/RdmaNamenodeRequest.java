package com.harana.datagrid.namenode.rdma;

import java.nio.ByteBuffer;

import com.harana.datagrid.namenode.NamenodeProtocol;
import com.harana.datagrid.namenode.NamenodeRequest;
import com.harana.datagrid.rpc.darpc.DaRPCMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RdmaNamenodeRequest implements DaRPCMessage {
	public static final Logger logger = LogManager.getLogger();
	public static final int CSIZE = 4 + Math.max(NamenodeRequest.SetFile.CSIZE, NamenodeRequest.RenameFile.CSIZE);
	
	private short cmd;
	private short type;
	private NamenodeRequest.CreateFile createFileReq;
	private NamenodeRequest.GetFile fileReq;
	private NamenodeRequest.SetFile setFileReq;
	private NamenodeRequest.RemoveFile removeReq;
	private NamenodeRequest.RenameFile renameFileReq;
	private NamenodeRequest.GetBlock getBlockReq;
	private NamenodeRequest.GetLocation getLocationReq;
	private NamenodeRequest.SetBlock setBlockReq;
	private NamenodeRequest.GetDataNode getDataNodeReq;
	private NamenodeRequest.DumpNameNode dumpNameNodeReq;
	private NamenodeRequest.PingNameNode pingNameNodeReq;

	public RdmaNamenodeRequest() {
		this.cmd = 0;
		this.type = 0;
		this.createFileReq = new NamenodeRequest.CreateFile();
		this.fileReq = new NamenodeRequest.GetFile();
		this.setFileReq = new NamenodeRequest.SetFile();
		this.removeReq = new NamenodeRequest.RemoveFile();
		this.renameFileReq = new NamenodeRequest.RenameFile();
		this.getBlockReq = new NamenodeRequest.GetBlock();
		this.getLocationReq = new NamenodeRequest.GetLocation();
		this.setBlockReq = new NamenodeRequest.SetBlock();
		this.dumpNameNodeReq = new NamenodeRequest.DumpNameNode();
		this.pingNameNodeReq = new NamenodeRequest.PingNameNode();
		this.getDataNodeReq = new NamenodeRequest.GetDataNode();
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.CreateFile message) {
		this.type = message.getType();
		this.createFileReq = message;
	}
	public RdmaNamenodeRequest(NamenodeRequest.GetFile message) {
		this.type = message.getType();
		this.fileReq = message;
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.SetFile message) {
		this.type = message.getType();
		this.setFileReq = message;
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.RemoveFile message) {
		this.type = message.getType();
		this.removeReq = message;
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.RenameFile message) {
		this.type = message.getType();
		this.renameFileReq = message;
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.GetBlock message) {
		this.type = message.getType();
		this.getBlockReq = message;
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.GetLocation message) {
		this.type = message.getType();
		this.getLocationReq = message;
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.SetBlock message) {
		this.type = message.getType();
		this.setBlockReq = message;
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.GetDataNode message) {
		this.type = message.getType();
		this.getDataNodeReq = message;
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.DumpNameNode message) {
		this.type = message.getType();
		this.dumpNameNodeReq = message;
	}
	
	public RdmaNamenodeRequest(NamenodeRequest.PingNameNode message) {
		this.type = message.getType();
		this.pingNameNodeReq = message;
	}
	
	public void setCommand(short command) {
		this.cmd = command;
	}	

	public int size() {
		return CSIZE;
	}
	
	public int write(ByteBuffer buffer) {
		buffer.putShort(cmd);
		buffer.putShort(type);
		
		int written = 4;
		switch(type) {
		case NamenodeProtocol.REQ_CREATE_FILE:
			written += createFileReq.write(buffer);
			break;		
		case NamenodeProtocol.REQ_GET_FILE:
			written += fileReq.write(buffer);
			break;
		case NamenodeProtocol.REQ_SET_FILE:
			written += setFileReq.write(buffer);
			break;
		case NamenodeProtocol.REQ_REMOVE_FILE:
			written += removeReq.write(buffer);
			break;			
		case NamenodeProtocol.REQ_RENAME_FILE:
			written += renameFileReq.write(buffer);
			break;
		case NamenodeProtocol.REQ_GET_BLOCK:
			written += getBlockReq.write(buffer);
			break;
		case NamenodeProtocol.REQ_GET_LOCATION:
			written += getLocationReq.write(buffer);
			break;			
		case NamenodeProtocol.REQ_SET_BLOCK:
			written += setBlockReq.write(buffer);
			break;
		case NamenodeProtocol.REQ_GET_DATANODE:
			written += getDataNodeReq.write(buffer);
			break;				
		case NamenodeProtocol.REQ_DUMP_NAMENODE:
			written += dumpNameNodeReq.write(buffer);
			break;
		case NamenodeProtocol.REQ_PING_NAMENODE:
			written += pingNameNodeReq.write(buffer);
			break;
		}
		
		return written;
	}
	
	public void update(ByteBuffer buffer) {
		this.cmd = buffer.getShort();
		this.type = buffer.getShort();
		
		switch(type) {
		case NamenodeProtocol.REQ_CREATE_FILE:
			createFileReq.update(buffer);
			break;		
		case NamenodeProtocol.REQ_GET_FILE:
			fileReq.update(buffer);
			break;
		case NamenodeProtocol.REQ_SET_FILE:
			setFileReq.update(buffer);
			break;
		case NamenodeProtocol.REQ_REMOVE_FILE:
			removeReq.update(buffer);
			break;			
		case NamenodeProtocol.REQ_RENAME_FILE:
			renameFileReq.update(buffer);
			break;
		case NamenodeProtocol.REQ_GET_BLOCK:
			getBlockReq.update(buffer);
			break;
		case NamenodeProtocol.REQ_GET_LOCATION:
			getLocationReq.update(buffer);
			break;			
		case NamenodeProtocol.REQ_SET_BLOCK:
			setBlockReq.update(buffer);
			break;
		case NamenodeProtocol.REQ_GET_DATANODE:
			getDataNodeReq.update(buffer);
			break;				
		case NamenodeProtocol.REQ_DUMP_NAMENODE:
			dumpNameNodeReq.update(buffer);
			break;		
		case NamenodeProtocol.REQ_PING_NAMENODE:
			pingNameNodeReq.update(buffer);
			break;
		}
	}

	public short getCmd() {
		return cmd;
	}
	
	public short getType() {
		return type;
	}

	public NamenodeRequest.CreateFile createFile() {
		return this.createFileReq;
	}
	
	public NamenodeRequest.GetFile getFile() {
		return fileReq;
	}
	
	public NamenodeRequest.SetFile setFile() {
		return setFileReq;
	}

	public NamenodeRequest.RemoveFile removeFile() {
		return removeReq;
	}	

	public NamenodeRequest.RenameFile renameFile() {
		return renameFileReq;
	}

	public NamenodeRequest.GetBlock getBlock() {
		return getBlockReq;
	}
	
	public NamenodeRequest.GetLocation getLocation() {
		return getLocationReq;
	}	

	public NamenodeRequest.SetBlock setBlock() {
		return setBlockReq;
	}

	public NamenodeRequest.GetDataNode getDataNode() {
		return this.getDataNodeReq;
	}	
	
	public NamenodeRequest.DumpNameNode dumpNameNode() {
		return this.dumpNameNodeReq;
	}
	
	public NamenodeRequest.PingNameNode pingNameNode() {
		return this.pingNameNodeReq;
	}
}
