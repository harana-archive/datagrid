package com.harana.datagrid.datanode.object;

import io.netty.buffer.ByteBuf;
import com.harana.datagrid.DatagridBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ObjectStoreUtils {
	private static Logger logger = LogManager.getLogger();

	private ObjectStoreUtils() {
	}

	public static int readStreamIntoHeapByteBuffer(InputStream src, ByteBuffer dst)	throws IOException {
		int readBytes = 0, writtenBytes = 0;
		int startPos = dst.position();
		int endPos = dst.limit();
		byte[] array = dst.array();
		try {
			while ((readBytes = src.read(array, writtenBytes, endPos - writtenBytes)) != -1) {
				writtenBytes += readBytes;
			}
		} catch (IOException e) {
			// this could happen if the block size is reduced without deleting existing objects
			logger.error("Got exception while trying to write into ByteBuffer " + dst + ": ", e);
			logger.error("DatagridBuffer start = {}, pos = {}, capacity = {}, written bytes = {}, read bytes = {}", startPos, dst.position(), dst.capacity(), writtenBytes, readBytes);
			logger.error("Read truncated to {} bytes", writtenBytes);
			throw e;
		}

		logger.debug("Written {} bytes to ByteBuffer range ({} - {})", writtenBytes, startPos, dst.position());
		return writtenBytes;
	}

	public static int readStreamIntoDirectByteBuffer(InputStream src, byte stagingBuffer[], DatagridBuffer dst) throws IOException {
		int readBytes, writtenBytes = 0;
		int startPos = dst.position();
		long t1 = 0, t2;
		if (ObjectStoreConstants.PROFILE) {
			t1 = System.nanoTime();
		}
		while ((readBytes = src.read(stagingBuffer)) != -1) {
			try {
				dst.put(stagingBuffer, 0, readBytes);
			} catch (java.nio.BufferOverflowException e) {
				// this could happen if the block size is reduced without deleting existing objects
				logger.error("Got exception while trying to write into ByteBuffer " + dst + ": ", e);
				logger.error("DatagridBuffer start = {}, pos = {}, capacity = {}, written bytes = {}, read bytes = {}",
						startPos, dst.position(), dst.capacity(), writtenBytes, readBytes);
				logger.error("Read truncated to {} bytes", writtenBytes);
				break;
			}
			if (com.harana.datagrid.datanode.object.ObjectStoreConstants.PROFILE) {
				t2 = System.nanoTime();
				logger.debug("Read {} bytes into output ByteBuffer, Latency={} (us)", readBytes, (t2 - t1) / 1000.);
				t1 = t2;
			}
			writtenBytes += readBytes;
		}
		logger.debug("Written {} bytes to ByteBuffer range ({} - {})", writtenBytes, startPos, dst.position());
		return writtenBytes;
	}

	public static void putZeroes(DatagridBuffer buf, int count) {
		while (count > 0) {
			buf.putInt(0);
			count -= 4;
		}
	}

	public static void showByteBufferContent(ByteBuffer buf, int offset, int bytes) {
		// dump the content of first bytes from the payload
		if (buf != null) {
			logger.debug("DUMP: TID:" + Thread.currentThread().getId() + " NioByteBuffer : " + buf);
			int min = (buf.limit() - offset);
			if (min > bytes) min = bytes;
			String str = "DUMP: TID:" + Thread.currentThread().getId() + " DUMP (" + offset + " ,+" + min + ") : ";
			min += offset;
			for (int i = offset; i < min; i++) {
				//str += Character.toHexString();
				str += Byte.toString(buf.get(i)) + " : ";
				if (i % 32 == 0) str += "\n";
			}
			logger.debug(str);
		} else {
			logger.debug("DUMP : payload content is NULL");
		}
	}

	public static void showByteBufContent(ByteBuf buf, int offset, int bytes) {
		// dump the content of first bytes from the payload
		if (buf != null) {
			int ori_rindex = buf.readerIndex();
			logger.debug("DUMP: TID:" + Thread.currentThread().getId() + " NettyByteBuf : " + buf);
			int min = (buf.capacity() - offset);
			if (min > bytes) min = bytes;
			String str = "DUMP: TID:" + Thread.currentThread().getId() + " DUMP (" + offset + " ,+" + min + ") : ";
			min += offset;
			for (int i = offset; i < min; i++) {
				//str += Character.toHexString();
				str += Byte.toString(buf.getByte(i)) + " : ";
				if (i % 32 == 0) str += "\n";
			}
			logger.debug(str);
			buf.readerIndex(ori_rindex);
		} else {
			logger.debug("DUMP : payload content is NULL");
		}
	}

	public static class ByteBufferBackedInputStream extends InputStream {
		private final DatagridBuffer buf;
		//private int mark;
		//private int readlimit;

		public ByteBufferBackedInputStream(DatagridBuffer buf) {
			logger.debug("New buffer");
			this.buf = buf;
		}

		public int read() {
			byte[] tmppbuf = new byte[1];
			if (!buf.hasRemaining()) {
				return -1;
			}
			int ret = read(tmppbuf);
			return (ret <= 0) ? -1 : (tmppbuf[0] & 0xff);
		}

		public int read(byte[] bytes) {
			if (!buf.hasRemaining()) {
				return -1;
			}
			int initialPos = buf.position();
			buf.get(bytes);
			int finalPos = buf.position();
			return finalPos - initialPos;
		}

		public int read(byte[] bytes, int off, int len) {
			if (!buf.hasRemaining()) {
				return -1;
			}
			len = Math.min(len, buf.remaining());
			buf.get(bytes, off, len);
			return len;
		}

		public long skip(long n) {
			int step = Math.min((int) n, buf.remaining());
			this.buf.position(buf.position() + step);
			return step;
		}

		public int available() {
			return buf.remaining();
		}

		public void mark(int readlimit) {
			//this.mark = this.buf.position();
			//this.readlimit = readlimit;
			//buf.mark();
		}

		public void reset() {
			//buf.position(this.mark);
			//buf.reset();
		}

		public boolean markSupported() {
			return true;
		}
	}
}