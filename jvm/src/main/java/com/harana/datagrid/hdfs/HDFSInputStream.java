package com.harana.datagrid.hdfs;

import com.harana.datagrid.core.streams.DatagridBufferedInputStream;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class HDFSInputStream extends FSDataInputStream {
	private static final Logger logger = LogManager.getLogger();
	
	private final DatagridBufferedInputStream inputStream;
	private Statistics stats;
	
	public HDFSInputStream(DatagridBufferedInputStream stream, Statistics stats) {
		super(new DatagridSeekable(stream, stats));
		logger.info("new HDFS stream");
		this.inputStream = stream;
	}

	@Override
	public long getPos() {
		return inputStream.position();
	}

	@Override
	public int read(ByteBuffer buf) throws IOException {
		int res = inputStream.read(buf);
		updateStats(res);
		return res;
	}

	@Override
	public int read(long position, byte[] buffer, int offset, int length)
			throws IOException {
		int res = inputStream.read(position, buffer, offset, length);
		updateStats(res);
		return res;
	}
	
	@Override
	public void readFully(long position, byte[] buffer) throws IOException {
		readFully(position, buffer, 0, buffer.length);
	}	

	@Override
	public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
		int nread = 0;
		while (nread < length) {
			int nbytes = read(position + nread, buffer, offset + nread, length - nread);
			if (nbytes < 0) {
				throw new java.io.EOFException("End of file reached before reading fully.");
			}
			nread += nbytes;
		}		
	}

	@Override
	public boolean seekToNewSource(long targetPos) {
		return false;
	}

	@Override
	public int read() throws IOException {
		int res = inputStream.read();
		updateStats(Integer.BYTES);
		return res;
	}

	@Override
	public int available() {
		return inputStream.available();
	}
	
	private void updateStats(long len) {
		if (stats != null && len > 0) {
			stats.incrementBytesRead(len);
		}
	}
	
	public static class DatagridSeekable extends InputStream implements Seekable, PositionedReadable, ByteBufferReadable {
		private final DatagridBufferedInputStream inputStream;
		private final Statistics stats;
		
		public DatagridSeekable(DatagridBufferedInputStream inputStream, Statistics stats) {
			this.inputStream = inputStream;
			this.stats = stats;
		}

		@Override
		public int read() throws IOException {
			int value = inputStream.read();
			updateStats(Integer.BYTES);
			return value;
		}

		@Override
		public int read(byte[] b) throws IOException {
			int res = inputStream.read(b);
			updateStats(Integer.BYTES);
			return res;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			int res = inputStream.read(b, off, len);
			updateStats(Integer.BYTES);
			return res;
		}

		@Override
		public long skip(long n) throws IOException {
			return inputStream.skip(n);
		}

		@Override
		public int available() {
			return inputStream.available();
		}

		@Override
		public void close() throws IOException {
			inputStream.close();
		}

		@Override
		public synchronized void mark(int readlimit) {
			inputStream.mark(readlimit);
		}

		@Override
		public synchronized void reset() throws IOException {
			inputStream.reset();
		}

		@Override
		public boolean markSupported() {
			return inputStream.markSupported();
		}

		@Override
		public int read(ByteBuffer dataBuf) throws IOException {
			int res = inputStream.read(dataBuf);
			updateStats(Integer.BYTES);
			return res;
		}

		@Override
		public int read(long position, byte[] buffer, int offset, int length)
				throws IOException {
			int res = inputStream.read(position, buffer, offset, length);
			updateStats(Integer.BYTES);
			return res;
		}

		@Override
		public void readFully(long position, byte[] buf) throws IOException {
			readFully(position, buf, 0, buf.length);
		}

		@Override
		public void readFully(long position, byte[] buffer, int offset, int length)
				throws IOException {
			int nread = 0;
			while (nread < length) {
				int nbytes = read(position + nread, buffer, offset + nread, length - nread);
				if (nbytes < 0) {
					throw new java.io.EOFException("End of file reached before reading fully.");
				}
				nread += nbytes;
			}
		}

		@Override
		public long getPos() {
			return inputStream.position();
		}

		@Override
		public void seek(long n) throws IOException {
			inputStream.seek(n);
		}

		@Override
		public boolean seekToNewSource(long targetPos) {
			return false;
		}
		
		private void updateStats(long len) {
			if (stats != null && len > 0) {
				stats.incrementBytesRead(len);
			}
		}		
	}
}