package com.harana.datagrid.namenode.rdma;

import java.nio.ByteBuffer;

import com.harana.datagrid.namenode.NamenodeState;
import com.harana.datagrid.namenode.NamenodeProtocol;
import com.harana.datagrid.namenode.NamenodeResponse;
import com.harana.datagrid.rpc.darpc.DaRPCMessage;

public class RdmaNamenodeResponse implements DaRPCMessage, NamenodeState {
	public static final int CSIZE = 4 + Math.max(NamenodeResponse.GetBlock.CSIZE, NamenodeResponse.RenameFile.CSIZE);
	
	private short type;
	private short error;
	private NamenodeResponse.Void voidRes;
	private NamenodeResponse.CreateFile createFileRes;
	private NamenodeResponse.GetFile getFileRes;
	private NamenodeResponse.DeleteFile delFileRes;
	private NamenodeResponse.RenameFile renameRes;
	private NamenodeResponse.GetBlock getBlockRes;
	private NamenodeResponse.GetLocation getLocationRes;
	private NamenodeResponse.GetDataNode getDataNodeRes;
	private NamenodeResponse.PingNameNode pingNameNodeRes;
	
	public RdmaNamenodeResponse() {
		this.type = 0;
		this.error = 0;
		
		this.voidRes = new NamenodeResponse.Void();
		this.createFileRes = new NamenodeResponse.CreateFile();
		this.getFileRes = new NamenodeResponse.GetFile();
		this.delFileRes = new NamenodeResponse.DeleteFile();
		this.renameRes = new NamenodeResponse.RenameFile();
		this.getBlockRes = new NamenodeResponse.GetBlock();
		this.getLocationRes = new NamenodeResponse.GetLocation();
		this.getDataNodeRes = new NamenodeResponse.GetDataNode();
		this.pingNameNodeRes = new NamenodeResponse.PingNameNode();
	}
	
	public RdmaNamenodeResponse(NamenodeResponse.Void message) {
		this.type = message.getType();
		this.voidRes = message;
	}
	
	public RdmaNamenodeResponse(NamenodeResponse.CreateFile message) {
		this.type = message.getType();
		this.createFileRes = message;
	}	
	
	public RdmaNamenodeResponse(NamenodeResponse.GetFile message) {
		this.type = message.getType();
		this.getFileRes = message;
	}
	
	public RdmaNamenodeResponse(NamenodeResponse.DeleteFile message) {
		this.type = message.getType();
		this.delFileRes = message;
	}	
	
	public RdmaNamenodeResponse(NamenodeResponse.RenameFile message) {
		this.type = message.getType();
		this.renameRes = message;
	}
	
	public RdmaNamenodeResponse(NamenodeResponse.GetBlock message) {
		this.type = message.getType();
		this.getBlockRes = message;
	}
	
	public RdmaNamenodeResponse(NamenodeResponse.GetLocation message) {
		this.type = message.getType();
		this.getLocationRes = message;
	}
	
	public RdmaNamenodeResponse(NamenodeResponse.GetDataNode message) {
		this.type = message.getType();
		this.getDataNodeRes = message;
	}	
	
	public RdmaNamenodeResponse(NamenodeResponse.PingNameNode message) {
		this.type = message.getType();
		this.pingNameNodeRes = message;
	}
	
	public void setType(short type) throws Exception {
		this.type = type;
		switch(type) {
		case NamenodeProtocol.RES_VOID:
			if (voidRes == null) {
				throw new Exception("Response type not set");
			}
			break;
		case NamenodeProtocol.RES_CREATE_FILE:
			if (createFileRes == null) {
				throw new Exception("Response type not set");
			}
			break;			
		case NamenodeProtocol.RES_GET_FILE:
			if (getFileRes == null) {
				throw new Exception("Response type not set");
			}
			break;
		case NamenodeProtocol.RES_DELETE_FILE:
			if (delFileRes == null) {
				throw new Exception("Response type not set");
			}
			break;			
		case NamenodeProtocol.RES_RENAME_FILE:
			if (renameRes == null) {
				throw new Exception("Response type not set");
			}
			break;			
		case NamenodeProtocol.RES_GET_BLOCK:
			if (getBlockRes == null) {
				throw new Exception("Response type not set");
			}
			break;
		case NamenodeProtocol.RES_GET_LOCATION:
			if (getLocationRes == null) {
				throw new Exception("Response type not set");
			}
			break;			
		case NamenodeProtocol.RES_GET_DATANODE:
			if (getDataNodeRes == null) {
				throw new Exception("Response type not set");
			}
			break;			
		case NamenodeProtocol.RES_PING_NAMENODE:
			if (pingNameNodeRes == null) {
				throw new Exception("Response type not set");
			}
			break;
		}		
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
	
	public NamenodeResponse.Void getVoid() {
		return voidRes;
	}	
	
	public NamenodeResponse.CreateFile createFile() {
		return createFileRes;
	}	
	
	public NamenodeResponse.GetFile getFile() {
		return getFileRes;
	}
	
	public NamenodeResponse.DeleteFile delFile() {
		return delFileRes;
	}	
	
	public NamenodeResponse.RenameFile getRename() {
		return renameRes;
	}	

	public NamenodeResponse.GetBlock getBlock() {
		return getBlockRes;
	}	
	
	public NamenodeResponse.GetLocation getLocation() {
		return getLocationRes;
	}	
	
	public NamenodeResponse.GetDataNode getDataNode() {
		return getDataNodeRes;
	}	
	
	public NamenodeResponse.PingNameNode pingNameNode() {
		return this.pingNameNodeRes;
	}
}
