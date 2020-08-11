package com.harana.datagrid.datanode.tcp;

import java.nio.ByteBuffer;

import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.rpc.narpc.NaRPCMessage;

public class TcpDatanodeRequest implements NaRPCMessage {
	public static final int HEADER_SIZE = Integer.BYTES;
	public static final int CSIZE = HEADER_SIZE + Math.max(WriteRequest.CSIZE, ReadRequest.CSIZE);
	
	private int type;
	private WriteRequest writeRequest;
	private ReadRequest readRequest;
	
	public TcpDatanodeRequest() {
		writeRequest = new WriteRequest();
		readRequest = new ReadRequest();
	}

	public TcpDatanodeRequest(WriteRequest writeRequest) {
		this.writeRequest = writeRequest;
		this.type = TcpDatanodeProtocol.REQ_WRITE;
	}

	public TcpDatanodeRequest(ReadRequest readRequest) {
		this.readRequest = readRequest;
		this.type = TcpDatanodeProtocol.REQ_READ;
	}

	public int size() {
		return CSIZE;
	}
	
	public int type() {
		return type;
	}

	@Override
	public void update(ByteBuffer buffer) {
		type = buffer.getInt();
		if (type == TcpDatanodeProtocol.REQ_WRITE) {
			writeRequest.update(buffer);
		} else if (type == TcpDatanodeProtocol.REQ_READ) {
			readRequest.update(buffer);
		}
	}

	@Override
	public int write(ByteBuffer buffer) {
		buffer.putInt(type);
		int written = HEADER_SIZE;
		if (type == TcpDatanodeProtocol.REQ_WRITE) {
			written += writeRequest.write(buffer);
		} else if (type == TcpDatanodeProtocol.REQ_READ) {
			written += readRequest.write(buffer);
		}
		return written;
	}
	
	public static class WriteRequest {
		public static final int FIELDS_SIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;
		public static final int CSIZE = FIELDS_SIZE + Integer.BYTES + (int) DatagridConstants.BLOCK_SIZE;
		private final ByteBuffer data;

		private int key;
		private long address;
		private int length;

		public WriteRequest() {
			data = ByteBuffer.allocateDirect((int) DatagridConstants.BLOCK_SIZE);
		}
		
		public WriteRequest(int key, long address, int length, ByteBuffer buffer) {
			this.key = key;
			this.address = address;
			this.length = length;
			this.data = buffer;
		}

		public long getAddress() {
			return address;
		}

		public int length() {
			return length;
		}
		
		public int getKey() {
			return key;
		}

		public ByteBuffer getBuffer() {
			return data;
		}

		public int size() {
			return CSIZE;
		}
		
		public void update(ByteBuffer buffer) {
			key = buffer.getInt();
			address = buffer.getLong();
			length = buffer.getInt();
			int remaining = buffer.getInt();
			buffer.limit(buffer.position() + remaining);
			data.clear();
			data.put(buffer);
			data.flip();
		}

		public int write(ByteBuffer buffer) {
			buffer.putInt(key);
			buffer.putLong(address);
			buffer.putInt(length);
			buffer.putInt(data.remaining());
			int written = FIELDS_SIZE + Integer.BYTES + data.remaining(); 
			buffer.put(data);
			return written;
		}		
	}
	
	public static class ReadRequest {
		public static final int CSIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;
		
		private int key;
		private long address;
		private int length;
		
		public ReadRequest() {
			
		}
		
		public ReadRequest(int key, long address, int length) {
			this.key = key;
			this.address = address;
			this.length = length;
		}

		public long getAddress() {
			return address;
		}
		
		public int length() {
			return length;
		}		
		
		public int getKey() {
			return key;
		}

		public int size() {
			return CSIZE;
		}
		
		public void update(ByteBuffer buffer) {
			key = buffer.getInt();
			address = buffer.getLong();
			length = buffer.getInt();
		}

		public int write(ByteBuffer buffer) {
			buffer.putInt(key);
			buffer.putLong(address);
			buffer.putInt(length);
			return CSIZE;
		}		
	}

	public WriteRequest getWriteRequest() {
		return writeRequest;
	}

	public ReadRequest getReadRequest() {
		return readRequest;
	}
}