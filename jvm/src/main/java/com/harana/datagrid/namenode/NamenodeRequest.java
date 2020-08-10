package com.harana.datagrid.namenode;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.harana.datagrid.DataType;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.metadata.FileInfo;
import com.harana.datagrid.metadata.FileName;

public class NamenodeRequest {

	public static class CreateFile implements NamenodeProtocol.Message {
		public static int CSIZE = FileName.CSIZE + 16;
		
		protected FileName filename;
		protected DataType type;
		protected int storageClass;
		protected int locationClass;
		protected boolean enumerable;
		
		public CreateFile() {
			this.filename = new FileName();
			this.type = DataType.DATAFILE;
			this.storageClass = 0;
			this.locationClass = 0;
			this.enumerable = true;
		}
		
		public CreateFile(FileName filename, DataType type, int storageClass, int locationClass, boolean enumerable) {
			this.filename = filename;
			this.type = type;
			this.storageClass = storageClass;
			this.locationClass = locationClass;
			this.enumerable = enumerable;
		}

		public FileName getFileName() {
			return filename;
		}

		public DataType getFileType() {
			return type;
		}
		
		public int getStorageClass() {
			return storageClass;
		}		
		
		public int getLocationClass() {
			return locationClass;
		}
		
		public boolean isEnumerable() {
			return enumerable;
		}

		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.REQ_CREATE_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			filename.write(buffer);
			buffer.putInt(type.getLabel());
			buffer.putInt(storageClass);
			buffer.putInt(locationClass);
			buffer.putInt(enumerable ? 1 : 0);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			filename.update(buffer);
			int _type = buffer.getInt();
			type = DataType.parse(_type);
			storageClass = buffer.getInt();
			locationClass = buffer.getInt();
			int _enumerable = buffer.getInt();
			enumerable = (_enumerable == 1);
		}

		@Override
		public String toString() {
			return "CreateFileReq [filename=" + filename + ", type=" + type + ", storageClass=" + storageClass + ", locationClass=" + locationClass + ", enumerable=" + enumerable + "]";
		}
	}
	
	public static class GetFile implements NamenodeProtocol.Message {
		public static int CSIZE = FileName.CSIZE + 4;
		
		protected FileName filename;
		protected boolean writeable;
		
		public GetFile() {
			this.filename = new FileName();
			this.writeable = false;
		}
		
		public GetFile(FileName filename, boolean writeable) {
			this.filename = filename;
			this.writeable = writeable;
		}

		public FileName getFileName() {
			return filename;
		}

		public boolean isWriteable() {
			return writeable;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.REQ_GET_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			filename.write(buffer);
			buffer.putInt(writeable ? 1 : 0);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			filename.update(buffer);
			int tmp = buffer.getInt();
			writeable = (tmp == 1);
		}		
	}
	
	public static class SetFile implements NamenodeProtocol.Message {
		public static int CSIZE = FileInfo.CSIZE + 4;
		
		protected FileInfo fileInfo;
		protected boolean close;
		
		public SetFile() {
			this.fileInfo = new FileInfo();
			this.close = false;
		}
		
		public SetFile(FileInfo fileInfo, boolean close) {
			this.fileInfo = fileInfo;
			this.close = close;
		}

		public FileInfo getFileInfo() {
			return fileInfo;
		}

		public boolean isClose() {
			return close;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.REQ_SET_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			fileInfo.write(buffer, true);
			buffer.putInt(close ? 1 : 0);
			return CSIZE;
		}		
	
		public void update(ByteBuffer buffer) {
			try {
				fileInfo.update(buffer);
				int tmp = buffer.getInt();
				close = (tmp == 1);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		@Override
		public String toString() {
			return "SetFileReq [fileInfo=" + fileInfo + ", close=" + close + "]";
		}		
	}
	
	public static class RemoveFile implements NamenodeProtocol.Message {
		public static int CSIZE = FileName.CSIZE + 4;
		
		protected FileName filename;
		protected boolean recursive;
		
		public RemoveFile() {
			this.filename = new FileName();
			this.recursive = false;
		}
		
		public RemoveFile(FileName filename, boolean recursive) {
			this.filename = filename;
			this.recursive = recursive;
		}

		public FileName getFileName() {
			return filename;
		}

		public boolean isRecursive() {
			return recursive;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.REQ_REMOVE_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			filename.write(buffer);
			buffer.putInt(recursive ? 1 : 0);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			filename.update(buffer);
			int tmp = buffer.getInt();
			recursive = (tmp == 1);
		}		
	}	
	
	public static class RenameFile implements NamenodeProtocol.Message {
		public static int CSIZE = FileName.CSIZE*2;
		
		protected FileName srcFileName;
		protected FileName dstFileName;

		public RenameFile() {
			this.srcFileName = new FileName();
			this.dstFileName = new FileName();
		}
		
		public RenameFile(FileName srcFileName, FileName dstFileName) {
			this.srcFileName = srcFileName;
			this.dstFileName = dstFileName;
		}

		public FileName getSrcFileName() {
			return srcFileName;
		}

		public FileName getDstFileName() {
			return dstFileName;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.REQ_RENAME_FILE;
		}		
		
		public int write(ByteBuffer buffer) {
			int written = srcFileName.write(buffer);
			written += dstFileName.write(buffer);
			return written;
		}		

		public void update(ByteBuffer buffer) {
			srcFileName.update(buffer);
			dstFileName.update(buffer);
		}

		@Override
		public String toString() {
			return "RenameFileReq [srcFileName=" + srcFileName
					+ ", dstFileName=" + dstFileName + "]";
		}		
	}	
	
	public static class GetBlock implements NamenodeProtocol.Message {
		public static int CSIZE = 32;
		
		protected long fd;
		protected long token;
		protected long position;
		protected long capacity;

		public GetBlock() {
			this.fd = 0;
			this.token = 0;
			this.position = 0;
			this.capacity = 0;	
		}
		
		public GetBlock(long fd, long token, long position, long capacity) {
			this.fd = fd;
			this.token = token;
			this.position = position;
			this.capacity = capacity;
		}

		public long getFd() {
			return fd;
		}

		public long getPosition() {
			return this.position;
		}

		public long getToken() {
			return token;
		}
		
		public long getCapacity() {
			return capacity;
		}
		
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.REQ_GET_BLOCK;
		}		
		
		public int write(ByteBuffer buffer) {
			buffer.putLong(fd);
			buffer.putLong(token);
			buffer.putLong(position);
			buffer.putLong(capacity);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			fd = buffer.getLong();
			token = buffer.getLong();
			position = buffer.getLong();
			capacity = buffer.getLong();
		}

		@Override
		public String toString() {
			return "GetBlockReq [fd=" + fd + ", token=" + token + ", position="
					+ position + ", capacity=" + capacity + "]";
		}

		public void setToken(long value) {
			this.token = value;
		}		
	}
	
	public static class GetLocation implements NamenodeProtocol.Message {
		public static int CSIZE = FileName.CSIZE + 8;
		
		protected FileName fileName;
		protected long position;

		public GetLocation() {
			this.fileName = new FileName();
			this.position = 0;
		}
		
		public GetLocation(FileName fileName, long position) {
			this.fileName = fileName;
			this.position = position;
		}

		public long getPosition() {
			return this.position;
		}

		public FileName getFileName() {
			return fileName;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.REQ_GET_LOCATION;
		}		
		
		public int write(ByteBuffer buffer) {
			fileName.write(buffer);
			buffer.putLong(position);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			fileName.update(buffer);
			position = buffer.getLong();
		}		
	}
	
	public static class SetBlock implements NamenodeProtocol.Message {
		public static int CSIZE = BlockInfo.CSIZE;
		
		protected BlockInfo blockInfo;
		
		public SetBlock() {
			this.blockInfo = new BlockInfo();
		}
		
		public SetBlock(BlockInfo blockInfo) {
			this.blockInfo = blockInfo;
		}

		public BlockInfo getBlockInfo() {
			return blockInfo;
		}
		
		public int size() {
			return CSIZE;
		}	
		
		public short getType() {
			return NamenodeProtocol.REQ_SET_BLOCK;
		}		
		
		public int write(ByteBuffer buffer) {
			return blockInfo.write(buffer);
		}
		
		public void update(ByteBuffer buffer) {
			blockInfo.update(buffer);
		}

		@Override
		public String toString() {
			return "SetBlockReq [blockInfo=" + blockInfo + "]";
		}
	}	
	
	public static class GetDataNode implements NamenodeProtocol.Message {
		public static int CSIZE = DatanodeInfo.CSIZE;
		
		protected DatanodeInfo dnInfo;
		
		public GetDataNode() {
			this.dnInfo = new DatanodeInfo();
		}
		
		public GetDataNode(DatanodeInfo dnInfo) {
			this.dnInfo = dnInfo;
		}

		public DatanodeInfo getInfo() {
			return this.dnInfo;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.REQ_GET_DATANODE;
		}		
		
		public int write(ByteBuffer buffer) {
			return dnInfo.write(buffer);
		}		

		public void update(ByteBuffer buffer) {
			dnInfo.update(buffer);
		}
	}	

	public static class DumpNameNode implements NamenodeProtocol.Message {
		public static int CSIZE = 4;
		
		protected int op;
		
		public DumpNameNode() {
			this.op = 0;
		}

		public int getOp() {
			return op;
		}
		
		public int size() {
			return CSIZE;
		}
		
		public short getType() {
			return NamenodeProtocol.REQ_DUMP_NAMENODE;
		}		
		
		public int write(ByteBuffer buffer) {
			buffer.putInt(op);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			op = buffer.getInt();
		}		
	}
	
	public static class PingNameNode implements NamenodeProtocol.Message {
		public static int CSIZE = 4;
		
		protected int op;
		
		public PingNameNode() {
			this.op = 0;	
		}
		
		public int getOp() {
			return this.op;
		}
		
		public int size() {
			return CSIZE;
		}
	
		public short getType() {
			return NamenodeProtocol.REQ_PING_NAMENODE;
		}		
		
		public int write(ByteBuffer buffer) {
			buffer.putInt(op);
			return CSIZE;
		}		

		public void update(ByteBuffer buffer) {
			op = buffer.getInt();
		}		
	}
}