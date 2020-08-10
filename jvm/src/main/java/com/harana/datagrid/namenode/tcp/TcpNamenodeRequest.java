package com.harana.datagrid.namenode.tcp;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.harana.datagrid.namenode.NamenodeProtocol;
import com.harana.datagrid.namenode.NamenodeRequest;
import com.harana.datagrid.rpc.narpc.NaRPCMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpNamenodeRequest extends NamenodeRequest implements NaRPCMessage {
	public static final Logger logger = LogManager.getLogger();
	public static final int CSIZE = 2*Short.BYTES + Math.max(SetFile.CSIZE, RenameFile.CSIZE);
	
	private short cmd;
	private short type;
	private CreateFile createFileReq;
	private GetFile fileReq;
	private SetFile setFileReq;
	private RemoveFile removeReq;
	private RenameFile renameFileReq;
	private GetBlock getBlockReq;
	private GetLocation getLocationReq;
	private SetBlock setBlockReq;
	private GetDataNode getDataNodeReq;
	private DumpNameNode dumpNameNodeReq;
	private PingNameNode pingNameNodeReq;

	public TcpNamenodeRequest() {
		this.cmd = 0;
		this.type = 0;
		this.createFileReq = new CreateFile();
		this.fileReq = new GetFile();
		this.setFileReq = new SetFile();
		this.removeReq = new RemoveFile();
		this.renameFileReq = new RenameFile();
		this.getBlockReq = new GetBlock();
		this.getLocationReq = new GetLocation();
		this.setBlockReq = new SetBlock();
		this.dumpNameNodeReq = new DumpNameNode();
		this.pingNameNodeReq = new PingNameNode();
		this.getDataNodeReq = new GetDataNode();
	}	
	
	public TcpNamenodeRequest(CreateFile message) {
		this.type = message.getType();
		this.createFileReq = message;
	}
	public TcpNamenodeRequest(GetFile message) {
		this.type = message.getType();
		this.fileReq = message;
	}
	
	public TcpNamenodeRequest(SetFile message) {
		this.type = message.getType();
		this.setFileReq = message;
	}
	
	public TcpNamenodeRequest(RemoveFile message) {
		this.type = message.getType();
		this.removeReq = message;
	}
	
	public TcpNamenodeRequest(RenameFile message) {
		this.type = message.getType();
		this.renameFileReq = message;
	}
	
	public TcpNamenodeRequest(GetBlock message) {
		this.type = message.getType();
		this.getBlockReq = message;
	}
	
	public TcpNamenodeRequest(GetLocation message) {
		this.type = message.getType();
		this.getLocationReq = message;
	}
	
	public TcpNamenodeRequest(SetBlock message) {
		this.type = message.getType();
		this.setBlockReq = message;
	}
	
	public TcpNamenodeRequest(GetDataNode message) {
		this.type = message.getType();
		this.getDataNodeReq = message;
	}
	
	public TcpNamenodeRequest(DumpNameNode message) {
		this.type = message.getType();
		this.dumpNameNodeReq = message;
	}
	
	public TcpNamenodeRequest(PingNameNode message) {
		this.type = message.getType();
		this.pingNameNodeReq = message;
	}
	
	public void setCommand(short command) {
		this.cmd = command;
	}	

	public int size() {
		return CSIZE;
	}
	
	public int write(ByteBuffer buffer) throws IOException{
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
	
	public void update(ByteBuffer buffer) throws IOException {
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
	
	public CreateFile createFile() {
		return this.createFileReq;
	}
	
	public GetFile getFile() {
		return fileReq;
	}
	
	public SetFile setFile() {
		return setFileReq;
	}

	public RemoveFile removeFile() {
		return removeReq;
	}	

	public RenameFile renameFile() {
		return renameFileReq;
	}

	public GetBlock getBlock() {
		return getBlockReq;
	}
	
	public GetLocation getLocation() {
		return getLocationReq;
	}	

	public SetBlock setBlock() {
		return setBlockReq;
	}

	public GetDataNode getDataNode() {
		return this.getDataNodeReq;
	}	
	
	public DumpNameNode dumpNameNode() {
		return this.dumpNameNodeReq;
	}
	
	public PingNameNode pingNameNode() {
		return this.pingNameNodeReq;
	}
}
