package com.harana.datagrid.namenode.tcp;

import java.nio.ByteBuffer;

import com.harana.datagrid.namenode.NamenodeState;
import com.harana.datagrid.namenode.NamenodeProtocol;
import com.harana.datagrid.namenode.NamenodeResponse;
import com.harana.datagrid.rpc.narpc.NaRPCMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TcpNamenodeResponse extends NamenodeResponse implements NamenodeState, NaRPCMessage {
	public static final Logger logger = LogManager.getLogger();
	public static final int CSIZE = 2*Short.BYTES + Math.max(GetBlock.CSIZE, RenameFile.CSIZE);
	
	private short type;
	private short error;
	private Void voidRes;
	private CreateFile createFileRes;
	private GetFile getFileRes;
	private DeleteFile delFileRes;
	private RenameFile renameRes;
	private GetBlock getBlockRes;
	private GetLocation getLocationRes;
	private GetDataNode getDataNodeRes;
	private NamenodeResponse.PingNameNode pingNameNodeRes;
	
	public TcpNamenodeResponse() {
		this.type = 0;
		this.error = 0;
		this.voidRes = new Void();
		this.createFileRes = new CreateFile();
		this.getFileRes = new GetFile();
		this.delFileRes = new DeleteFile();
		this.renameRes = new RenameFile();
		this.getBlockRes = new GetBlock();
		this.getLocationRes = new GetLocation();
		this.getDataNodeRes = new GetDataNode();
		this.pingNameNodeRes = new NamenodeResponse.PingNameNode();
	}
	
	public TcpNamenodeResponse(Void message) {
		this.type = message.getType();
		this.voidRes = message;
	}
	
	public TcpNamenodeResponse(CreateFile message) {
		this.type = message.getType();
		this.createFileRes = message;
	}	
	
	public TcpNamenodeResponse(GetFile message) {
		this.type = message.getType();
		this.getFileRes = message;
	}
	
	public TcpNamenodeResponse(DeleteFile message) {
		this.type = message.getType();
		this.delFileRes = message;
	}	
	
	public TcpNamenodeResponse(RenameFile message) {
		this.type = message.getType();
		this.renameRes = message;
	}
	
	public TcpNamenodeResponse(GetBlock message) {
		this.type = message.getType();
		this.getBlockRes = message;
	}
	
	public TcpNamenodeResponse(GetLocation message) {
		this.type = message.getType();
		this.getLocationRes = message;
	}
	
	public TcpNamenodeResponse(GetDataNode message) {
		this.type = message.getType();
		this.getDataNodeRes = message;
	}	
	
	public TcpNamenodeResponse(NamenodeResponse.PingNameNode message) {
		this.type = message.getType();
		this.pingNameNodeRes = message;
	}
	
	public void setType(short type) throws Exception {
		this.type = type;
	}	

	public int size() {
		return CSIZE;
	}
	
	public int write(ByteBuffer buffer) {
		buffer.putShort(type);
		buffer.putShort(error);
		
		int written = 4;
		switch(type) {
		case NamenodeProtocol.RES_VOID:
			written += voidRes.write(buffer);
			break;	
		case NamenodeProtocol.RES_CREATE_FILE:
			written += createFileRes.write(buffer);
			break;				
		case NamenodeProtocol.RES_GET_FILE:
			written += getFileRes.write(buffer);
			break;
		case NamenodeProtocol.RES_DELETE_FILE:
			written += delFileRes.write(buffer);
			break;				
		case NamenodeProtocol.RES_RENAME_FILE:
			written += renameRes.write(buffer);
			break;				
		case NamenodeProtocol.RES_GET_BLOCK:
			written += getBlockRes.write(buffer);
			break;
		case NamenodeProtocol.RES_GET_LOCATION:
			written += getLocationRes.write(buffer);
			break;			
		case NamenodeProtocol.RES_GET_DATANODE:
			written += getDataNodeRes.write(buffer);
			break;			
		case NamenodeProtocol.RES_PING_NAMENODE:
			written += pingNameNodeRes.write(buffer);
			break;			
		}
		
		return written;
	}
	
	public void update(ByteBuffer buffer) {
		this.type = buffer.getShort();
		this.error = buffer.getShort();
		
		switch(type) {
		case NamenodeProtocol.RES_VOID:
			voidRes.update(buffer);
			voidRes.setError(error);
			break;			
		case NamenodeProtocol.RES_CREATE_FILE:
			createFileRes.update(buffer);
			createFileRes.setError(error);
			break;				
		case NamenodeProtocol.RES_GET_FILE:
			getFileRes.update(buffer);
			getFileRes.setError(error);
			break;	
		case NamenodeProtocol.RES_DELETE_FILE:
			delFileRes.update(buffer);
			delFileRes.setError(error);
			break;				
		case NamenodeProtocol.RES_RENAME_FILE:
			renameRes.update(buffer);
			renameRes.setError(error);
			break;				
		case NamenodeProtocol.RES_GET_BLOCK:
			getBlockRes.update(buffer);
			getBlockRes.setError(error);
			break;
		case NamenodeProtocol.RES_GET_LOCATION:
			getLocationRes.update(buffer);
			getLocationRes.setError(error);
			break;			
		case NamenodeProtocol.RES_GET_DATANODE:
			getDataNodeRes.update(buffer);
			getDataNodeRes.setError(error);
			break;			
		case NamenodeProtocol.RES_PING_NAMENODE:
			pingNameNodeRes.update(buffer);
			pingNameNodeRes.setError(error);
			break;		
		}
	}
	
	public short getType() {
		return type;
	}
	
	public short getError() {
		return error;
	}

	public void setError(short error) {
		this.error = error;
	}	
	
	public Void getVoid() {
		return voidRes;
	}	
	
	public CreateFile createFile() {
		return createFileRes;
	}	
	
	public GetFile getFile() {
		return getFileRes;
	}
	
	public DeleteFile removeFile() {
		return delFileRes;
	}	
	
	public RenameFile renameFile() {
		return renameRes;
	}	

	public GetBlock getBlock() {
		return getBlockRes;
	}	
	
	public GetLocation getLocation() {
		return getLocationRes;
	}	
	
	public GetDataNode getDataNode() {
		return getDataNodeRes;
	}	
	
	public NamenodeResponse.PingNameNode pingNameNode() {
		return this.pingNameNodeRes;
	}
}