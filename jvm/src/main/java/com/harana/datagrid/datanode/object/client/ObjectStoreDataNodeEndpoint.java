package com.harana.datagrid.datanode.object.client;

import com.harana.datagrid.datanode.object.ObjectStoreConstants;
import com.harana.datagrid.datanode.object.ObjectStoreUtils;
import com.harana.datagrid.datanode.object.rpc.MappingEntry;
import com.harana.datagrid.datanode.object.rpc.ObjectStoreRPC;
import com.harana.datagrid.datanode.object.rpc.RPCCall;
import com.harana.datagrid.Buffer;
import com.harana.datagrid.conf.Constants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.client.datanode.DatanodeFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectStoreDataNodeEndpoint implements DatanodeEndpoint {
	private static final Logger logger = LogManager.getLogger();

	private final static AtomicInteger objectSequenceNumber = new AtomicInteger(0);
	private final ObjectStoreMetadataClient metadataClient;
	private final S3ObjectStoreClient objectStoreClient;
	private final String localObjectKeyPrefix;
	private final byte[] stagingBuffer = new byte[64 * 1024 * 1024];
	private final int blockSize = Long.valueOf(Constants.BLOCK_SIZE).intValue();

	public ObjectStoreDataNodeEndpoint(ObjectStoreMetadataClient metadataClient) throws IOException {
		logger.debug("TID {} : Creating a new ObjectStore client endpoint", Thread.currentThread().getId());
		this.metadataClient = metadataClient;
		this.objectStoreClient = new S3ObjectStoreClient();
		Random rand = new Random();
		localObjectKeyPrefix = ObjectStoreConstants.OBJECT_PREFIX + "-"
				+ InetAddress.getLocalHost().getHostName() + "-"
				+ Integer.toString(Math.abs(rand.nextInt())) + "-";
	}

	public DatanodeFuture write(Buffer buffer, BlockInfo blockInfo, long remoteOffset) {
		long startTime = 0, endTime;
		if (ObjectStoreConstants.PROFILE) {
			startTime = System.nanoTime();
		}
		String key = makeUniqueKey(blockInfo);
		int length = buffer.remaining();
		logger.debug("Block write: addr = {}, start offset = {}, end offset = {}, key = {}", blockInfo.getAddr(), remoteOffset, (remoteOffset + length), key);
		objectStoreClient.putObject(key, buffer);
		if (remoteOffset == 0 && length == ObjectStoreConstants.ALLOCATION_SIZE) {
			metadataClient.writeBlock(blockInfo, key);
		} else {
			metadataClient.writeBlockRange(blockInfo, remoteOffset, length, key);
		}
		if (ObjectStoreConstants.PROFILE) {
			endTime = System.nanoTime();
			logger.info("Wrote {} bytes in {} (us)\n", length, (endTime - startTime) / 1000.);
		}
		return new ObjectStoreDataFuture(length);
	}

	private String makeUniqueKey(BlockInfo blockInfo) {
		return localObjectKeyPrefix + Long.toString(blockInfo.getAddr() / Constants.BLOCK_SIZE)
				+ "-" + objectSequenceNumber.getAndIncrement();
	}

	public DatanodeFuture read(Buffer buffer, BlockInfo blockInfo, long remoteOffset)
			throws IOException {
		long startTime = 0, endTime;
		if (ObjectStoreConstants.PROFILE) {
			startTime = System.nanoTime();
		}
		int startPos = buffer.position();
		long curOffset = remoteOffset;
		final long endOffset = remoteOffset + buffer.limit();
		logger.debug("Block read request: Block address = {}, range ({} - {})", blockInfo.getAddr(), remoteOffset, endOffset);
		ObjectStoreRPC.TranslateBlock rpc;
		Future<RPCCall> future = metadataClient.translateBlock(blockInfo);
		try {
			rpc = (ObjectStoreRPC.TranslateBlock) future.get();
		} catch (Exception e) {
			logger.error("RPC exception: " + e);
			throw new IOException("Got exception while performing RPC call ", e);
		}
		List<MappingEntry> mapping = rpc.getResponse();
		if (mapping != null) {
			// A read of a non-written block is theoretically possible
			for (MappingEntry entry : mapping) {
				if (remoteOffset < entry.getEndOffset() && endOffset > entry.getStartOffset()) {
					// the current mapping entry overlaps with what we are interested in
					String key = entry.getKey();
					long objStartOffset = entry.getObjectOffset();
					long curLength = entry.getSize();
					if (curOffset > entry.getStartOffset()) {
						// unaligned object read, we need to skip the beginning of the object
						long shift = curOffset - entry.getStartOffset();
						objStartOffset += shift;
						curLength -= shift;
					} else if (curOffset < entry.getStartOffset()) {
						// we have a read hole (the range was never written)
						logger.warn("Reading non-initialized block range ({} - {})", curOffset, entry.getStartOffset());
						ObjectStoreUtils.putZeroes(buffer, (int) (entry.getStartOffset() - curOffset));
						curOffset = entry.getStartOffset();
					}
					if (entry.getEndOffset() >= endOffset) {
						// do not read end of block
						curLength -= (entry.getEndOffset() - endOffset);
					}
					assert curLength > 0;
					logger.debug("Block range ({} - {}) maps to object key {} range ({} - {})", curOffset, curOffset + curLength, key, objStartOffset, objStartOffset + curLength);
					InputStream input = this.objectStoreClient.getObject(key, objStartOffset, objStartOffset + curLength);
					long st = 0, et;
					if (ObjectStoreConstants.PROFILE) {
						st = System.nanoTime();
					}
					//if (buffer.()) {
					//	ObjectStoreUtils.readStreamIntoHeapByteBuffer(input, buffer);
					//} else {
					ObjectStoreUtils.readStreamIntoDirectByteBuffer(input, stagingBuffer, buffer);
					//}
					if (ObjectStoreConstants.PROFILE) {
						et = System.nanoTime();
						logger.debug("{} (us) for reading object into ByteBuffer", (et - st) / 1000.);
					}
					curOffset += curLength;
				}
			}
		}
		int endPos = buffer.position();
		int readLength = endPos - startPos;
		if (endPos < buffer.limit()) {
			/* NOTE: Not clear what to do if the buffer is not filled up to limit().
			 * Padding with zeroes might not be the correct behavior.
			 */
			logger.warn("Trying to read non-initialized block range ({} - {})", endPos, buffer.limit());
		}
		if (ObjectStoreConstants.PROFILE) {
			endTime = System.nanoTime();
			logger.info("Read {} bytes in {} (us)\n", readLength, (endTime - startTime) / 1000.);
		}
		return new ObjectStoreDataFuture(readLength);
	}

	public void close() {
		if (this.metadataClient != null) {
			this.metadataClient.close();
		}
		if (this.objectStoreClient != null) {
			this.objectStoreClient.close();
		}
	}

	public boolean isLocal() {
		return false;
	}

	protected void finalize() {
		logger.info("Closing ObjectStore DataNode Endpoint");
		try {
			close();
		} catch (Exception e) {
			logger.error("Could not close ObjectStoreEndpoint. Reason: {}", e);
		}
	}
}
