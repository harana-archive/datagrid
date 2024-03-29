package com.harana.datagrid.namenode;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.harana.datagrid.client.namenode.NamenodeVoid;
import com.harana.datagrid.client.namenode.responses.*;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeStatistics;
import com.harana.datagrid.metadata.FileInfo;

public class NamenodeResponse {
	public static class Void implements NamenodeProtocol.Message, NamenodeVoid {
		private short error;
		
		public Void() {
			this.error = 0;
		}
		
		public int size() {
			return 0;
		}
		
		public short getType() {
			return NamenodeProtocol.RES_VOID;
		}
		
		public void update(ByteBuffer buffer) {
		}

		public int write(ByteBuffer buffer) {
			return 0;
		}
		
		public short getError() {
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}		
	}
	
	public static class CreateFile implements NamenodeProtocol.Message, CreateFileResponse {
		public static int CSIZE = FileInfo.CSIZE*2 + BlockInfo.CSIZE*2;
		
		private final FileInfo fileInfo;
		private final FileInfo parentInfo;
		private final BlockInfo fileBlock;
		private final BlockInfo dirBlock;
		
		private boolean shipToken;
		private short error;
		

		public CreateFile() {
			this.fileInfo = new FileInfo();
			this.parentInfo = new FileInfo();
			this.fileBlock = new BlockInfo();
			this.dirBlock = new BlockInfo();
		
			this.shipToken = false;
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.RES_CREATE_FILE;
		}
		
		public int write(ByteBuffer buffer) {
			int written = fileInfo.write(buffer, shipToken);
			written += parentInfo.write(buffer, false);
			written += fileBlock.write(buffer);
			written += dirBlock.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				fileInfo.update(buffer);
				parentInfo.update(buffer);
				fileBlock.update(buffer);
				dirBlock.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		public FileInfo getFile() {
			return fileInfo;
		}

		public void setFileInfo(FileInfo fileInfo) {
			if (fileInfo != null) {
				this.fileInfo.setFileInfo(fileInfo);
			}
		}
		
		public FileInfo getParent() {
			return parentInfo;
		}
		
		public void setParentInfo(FileInfo parentInfo) {
			if (parentInfo != null) {
				this.parentInfo.setFileInfo(parentInfo);
			}
		}

		public BlockInfo getFileBlock() {
			return fileBlock;
		}
		
		public void setFileBlock(BlockInfo blockInfo) {
			if (blockInfo != null) {
				this.fileBlock.setBlockInfo(blockInfo);
			}
		}
		
		public BlockInfo getDirBlock() {
			return dirBlock;
		}
		
		public void setDirBlock(BlockInfo blockInfo) {
			if (blockInfo != null) {
				this.dirBlock.setBlockInfo(blockInfo);
			}
		}		
		
		public void shipToken(boolean value) {
			this.shipToken = value;
		}

		public short getError() {
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}

		public boolean isShipToken() {
			return shipToken;
		}
	}	
	
	public static class GetFile implements NamenodeProtocol.Message, GetFileResponse {
		public static int CSIZE = FileInfo.CSIZE + BlockInfo.CSIZE;
		
		private final FileInfo fileInfo;
		private final BlockInfo fileBlock;
		private boolean shipToken;
		private short error;

		public GetFile() {
			this.fileInfo = new FileInfo();
			this.fileBlock = new BlockInfo();
			
			this.shipToken = false;
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.RES_GET_FILE;
		}
		
		public int write(ByteBuffer buffer) {
			int written = fileInfo.write(buffer, shipToken);
			written += fileBlock.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				fileInfo.update(buffer);
				fileBlock.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		public FileInfo getFile() {
			return fileInfo;
		}

		public void setFileInfo(FileInfo fileInfo) {
			if (fileInfo != null) {
				this.fileInfo.setFileInfo(fileInfo);
				this.shipToken = false;
			}
		}
		
		public BlockInfo getFileBlock() {
			return fileBlock;
		}
		
		public void setFileBlock(BlockInfo blockInfo) {
			if (blockInfo != null) {
				fileBlock.setBlockInfo(blockInfo);
			}
		}
		
		public void shipToken() {
			this.shipToken = true;
		}

		public short getError() {
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}
	}
	
	public static class DeleteFile implements NamenodeProtocol.Message, DeleteFileResponse {
		public static int CSIZE = FileInfo.CSIZE*2;
		
		private final FileInfo fileInfo;
		private final FileInfo parentInfo;
		private boolean shipToken;
		
		private short error;

		public DeleteFile() {
			this.fileInfo = new FileInfo();
			this.parentInfo = new FileInfo();
			this.shipToken = false;
			
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.RES_DELETE_FILE;
		}
		
		public int write(ByteBuffer buffer) {
			int written = fileInfo.write(buffer, shipToken);
			written += parentInfo.write(buffer, false);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				fileInfo.update(buffer);
				parentInfo.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		public FileInfo getFile() {
			return fileInfo;
		}

		public FileInfo getParent() {
			return parentInfo;
		}
		
		public void setFileInfo(FileInfo fileInfo) {
			if (fileInfo != null) {
				this.fileInfo.setFileInfo(fileInfo);
				this.shipToken = false;
			}
		}
		
		public void setParentInfo(FileInfo parentInfo) {
			if (parentInfo != null) {
				this.parentInfo.setFileInfo(parentInfo);
			}
		}		
		
		public void shipToken() {
			this.shipToken = true;
		}

		public short getError() {
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}
	}	
	
	public static class RenameFile implements NamenodeProtocol.Message, RenameFileResponse {
		public static int CSIZE = FileInfo.CSIZE*4 + BlockInfo.CSIZE*2;
		
		private final FileInfo srcParent;
		private final FileInfo srcFile;
		private final BlockInfo srcBlock;
		private final FileInfo dstParent;
		private final FileInfo dstFile;
		private final BlockInfo dstBlock;
		private short error;

		public RenameFile() {
			this.srcParent = new FileInfo();
			this.srcFile = new FileInfo();
			this.srcBlock = new BlockInfo();
			this.dstParent = new FileInfo();
			this.dstFile = new FileInfo();	
			this.dstBlock = new BlockInfo();
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.RES_RENAME_FILE;
		}
		
		public int write(ByteBuffer buffer) {
			int written = srcParent.write(buffer, false);
			written += srcFile.write(buffer, false);
			written += srcBlock.write(buffer);
			written += dstParent.write(buffer, false);
			written += dstFile.write(buffer, false);
			written += dstBlock.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			try {
				srcParent.update(buffer);
				srcFile.update(buffer);
				srcBlock.update(buffer);
				dstParent.update(buffer);
				dstFile.update(buffer);
				dstBlock.update(buffer);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		
		public FileInfo getSrcParent() {
			return srcParent;
		}

		public FileInfo getSrcFile() {
			return srcFile;
		}

		public FileInfo getDstParent() {
			return dstParent;
		}	
		
		public FileInfo getDstFile() {
			return this.dstFile;
		}		
		
		public void setSrcParent(FileInfo srcParent) {
			if (srcParent != null) {
				this.srcParent.setFileInfo(srcParent);
			}
		}

		public void setSrcFile(FileInfo srcFile) {
			if (srcFile != null) {
				this.srcFile.setFileInfo(srcFile);
			}
		}

		public void setDstParent(FileInfo dstParent) {
			if (dstParent != null) {
				this.dstParent.setFileInfo(dstParent);
			}
		}

		public void setDstFile(FileInfo dstFile) {
			if (dstFile != null) {
				this.dstFile.setFileInfo(dstFile);
			}
		}
		
		public short getError() {
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}

		public void setSrcBlock(BlockInfo srcBlock) {
			if (srcBlock != null) {
				this.srcBlock.setBlockInfo(srcBlock);
			}
		}

		public void setDstBlock(BlockInfo dstBlock) {
			if (dstBlock != null) {
				this.dstBlock.setBlockInfo(dstBlock);
			}
		}

		public BlockInfo getSrcBlock() {
			return srcBlock;
		}

		public BlockInfo getDstBlock() {
			return dstBlock;
		}		
	}	

	public static class GetBlock implements NamenodeProtocol.Message, GetBlockResponse {
		public static int CSIZE = BlockInfo.CSIZE;
		
		private final BlockInfo blockInfo;
		private short error;
		
		public GetBlock() {
			this.blockInfo = new BlockInfo();
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.RES_GET_BLOCK;
		}
		
		public int write(ByteBuffer buffer) {
			return blockInfo.write(buffer);
		}		

		public void update(ByteBuffer buffer) {
			blockInfo.update(buffer);
		}

		public BlockInfo getBlockInfo() {
			return blockInfo;
		}

		public void setBlockInfo(BlockInfo blockInfo) {
			if (blockInfo != null) {
				this.blockInfo.setBlockInfo(blockInfo);
			} 
		}
		
		public short getError() {
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}
	}	
	
	public static class GetLocation implements NamenodeProtocol.Message, GetLocationResponse {
		public static int CSIZE = BlockInfo.CSIZE + 8;
		
		private final BlockInfo blockInfo;
		protected long fd;
		private short error;
		
		public GetLocation() {
			this.blockInfo = new BlockInfo();
			this.fd = 0;		
			this.error = 0;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.RES_GET_LOCATION;
		}
		
		public int write(ByteBuffer buffer) {
			return blockInfo.write(buffer);
		}		

		public void update(ByteBuffer buffer) {
			blockInfo.update(buffer);
		}

		public BlockInfo getBlockInfo() {
			return blockInfo;
		}

		public void setBlockInfo(BlockInfo blockInfo) {
			if (blockInfo != null) {
				this.blockInfo.setBlockInfo(blockInfo);
			} 
		}
		
		public long getFd() {
			return fd;
		}

		public void setFd(long fd) {
			this.fd = fd;
		}

		public short getError() {
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}		
	}	
	
	public static class GetDataNode implements NamenodeProtocol.Message, GetDataNodeResponse {
		public static int CSIZE = DatanodeStatistics.CSIZE;
		
		private final DatanodeStatistics statistics;

		public GetDataNode() {
			this.statistics = new DatanodeStatistics();
		}
		
		public void setError(short error) {
			
		}

		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.RES_GET_DATANODE;
		}	
		
		public int write(ByteBuffer buffer) {
			return statistics.write(buffer);
		}		

		public void update(ByteBuffer buffer) {
			statistics.update(buffer);
		}

		public DatanodeStatistics getStatistics() {
			return this.statistics;
		}
		
		public void setFreeBlockCount(int blockCount) {
			this.statistics.setFreeBlockCount(blockCount);
		}
		
		public short getError() {
			return 0;
		}

		public void setServiceId(long serviceId) {
			this.statistics.setServiceId(serviceId);
		}		
	}	
	
	public static class PingNameNode implements NamenodeProtocol.Message, PingResponse {
		public static int CSIZE = 4;
		
		private int data;
		private short error;
		
		public PingNameNode() {
			this.data = 0;
			this.error = 0;
		}

		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.RES_PING_NAMENODE;
		}
		
		public int write(ByteBuffer buffer) {
			buffer.putInt(data);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			data = buffer.getInt();
		}

		public int getData() {
			return data;
		}
		
		public void setData(int data) {
			this.data = data;
		}

		public short getError() {
			return error;
		}

		public void setError(short error) {
			this.error = error;
		}
	}
}