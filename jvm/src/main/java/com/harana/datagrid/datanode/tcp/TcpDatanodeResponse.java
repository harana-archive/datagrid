package com.harana.datagrid.datanode.tcp;

import java.nio.ByteBuffer;

import com.harana.datagrid.conf.Constants;
import com.harana.datagrid.rpc.narpc.NaRPCMessage;
import com.harana.datagrid.datanode.tcp.TcpDatanodeRequest.ReadRequest;
import com.harana.datagrid.datanode.tcp.TcpDatanodeRequest.WriteRequest;

public class TcpDatanodeResponse implements NaRPCMessage {
	public static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES;
	public static final int CSIZE = HEADER_SIZE + Math.max(WriteRequest.CSIZE, ReadRequest.CSIZE);
	
	private int error;
	private int type;
	private WriteResponse writeResponse;
	private ReadResponse readResponse;
	
	public TcpDatanodeResponse(WriteResponse writeResponse) {
		this.writeResponse = writeResponse;
		this.type = TcpDatanodeProtocol.REQ_WRITE;
		this.error = TcpDatanodeProtocol.RET_OK;
	}

	public TcpDatanodeResponse(ReadResponse readResponse) {
		this.readResponse = readResponse;
		this.type = TcpDatanodeProtocol.REQ_READ;
		this.error = TcpDatanodeProtocol.RET_OK;
	}

	public TcpDatanodeResponse(int error) {
		this.error = error;
	}

	public int size() {
		return CSIZE;
	}

	@Override
	public void update(ByteBuffer buffer) {
		error = buffer.getInt();
		type = buffer.getInt();
		if (type == TcpDatanodeProtocol.REQ_WRITE) {
			writeResponse.update(buffer);
		} else if (type == TcpDatanodeProtocol.REQ_READ) {
			readResponse.update(buffer);
		}
	}

	@Override
	public int write(ByteBuffer buffer) {
		buffer.putInt(error);
		buffer.putInt(type);
		int written = HEADER_SIZE;
		if (type == TcpDatanodeProtocol.REQ_WRITE) {
			written += writeResponse.write(buffer);
		} else if (type == TcpDatanodeProtocol.REQ_READ) {
			written += readResponse.write(buffer);
		}		
		return written;
	}
	
	public static class WriteResponse {
		private int size;
		
		public WriteResponse() {
			
		}
		
		public WriteResponse(int size) {
			this.size = size;
		}

		public int size() {
			return size;
		}
		
		public void update(ByteBuffer buffer) {
			size = buffer.getInt();
		}

		public int write(ByteBuffer buffer) {
			buffer.putInt(size);
			return 4;
		}		
	}
	
	public static class ReadResponse {
		public static final int CSIZE = Integer.BYTES + (int) Constants.BLOCK_SIZE;
		
		private final ByteBuffer data;
		
		public ReadResponse(ByteBuffer data) {
			this.data = data;
		}

		public int write(ByteBuffer buffer) {
			int written = data.remaining();
			buffer.putInt(data.remaining());
			buffer.put(data);
			return Integer.BYTES + written;
		}

		public void update(ByteBuffer buffer) {
			int remaining = buffer.getInt();
			data.clear().limit(remaining);
			buffer.limit(buffer.position() + remaining);
			data.put(buffer);
		}
		
		public int size() {
			return CSIZE;
		}		
	}
}