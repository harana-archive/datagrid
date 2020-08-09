package com.harana.datagrid.storage.nvmf.client;

import com.harana.datagrid.CrailBuffer;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.storage.StorageFuture;
import com.harana.datagrid.storage.StorageResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class NvmfUnalignedWriteFuture implements StorageFuture {
	private final NvmfStorageEndpoint endpoint;
	private StorageFuture beginFuture;
	private StorageFuture middleFuture;
	private StorageFuture endFuture;
	private final int written;
	private NvmfStagingBufferCache.BufferCacheEntry beginBuffer;
	private NvmfStagingBufferCache.BufferCacheEntry endBuffer;

	private final boolean isSectorAligned(long address) {
		return address % endpoint.getLBADataSize() == 0;
	}

	private final long floorToSectorSize(long address) {
		return address - offsetInSector(address);
	}

	private final int floorToSectorSize(int length) {
		return length - offsetInSector(length);
	}

	private final int leftInSector(long address) {
		return endpoint.getLBADataSize() - offsetInSector(address);
	}

	private final int offsetInSector(long address) {
		return (int)(address % endpoint.getLBADataSize());
	}

	NvmfUnalignedWriteFuture(NvmfStorageEndpoint endpoint, CrailBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws Exception {
		this.endpoint = endpoint;
		this.written = buffer.remaining();
		/* assume blockInfo.getAddr() is sector aligned */
		assert isSectorAligned(blockInfo.getAddr());

		long nextRemoteOffset = remoteOffset;
		/* beginning */
		if (!isSectorAligned(remoteOffset)) {
			int copySize = Math.min(leftInSector(remoteOffset), buffer.remaining());
			nextRemoteOffset = remoteOffset + copySize;
			int oldLimit = buffer.limit();
			buffer.limit(buffer.position() + copySize);
			long alignedRemoteOffset = floorToSectorSize(remoteOffset);
			long alignedRemoteAddress = blockInfo.getAddr() + alignedRemoteOffset;
			beginBuffer = endpoint.getStagingBufferCache().getExisting(alignedRemoteAddress);
			if (beginBuffer == null) {
				/* we had to delete the old buffer because we ran out of space. This should happen rarely. */
				beginBuffer = endpoint.getStagingBufferCache().get(alignedRemoteAddress);
				endpoint.read(beginBuffer.getBuffer(), blockInfo, alignedRemoteOffset).get();
			} else {
				/* Wait for previous end operation to finish */
				beginBuffer.getFuture().get();
			}
			CrailBuffer stagingBuffer = beginBuffer.getBuffer();
			stagingBuffer.position(offsetInSector(remoteOffset));
			stagingBuffer.getByteBuffer().put(buffer.getByteBuffer());
			buffer.limit(oldLimit);
			stagingBuffer.position(0);
			beginFuture = endpoint.write(stagingBuffer, blockInfo, alignedRemoteOffset);
			beginBuffer.setFuture(beginFuture);
			stagingBuffer.position(offsetInSector(remoteOffset));
		}

		/* middle */
		if (isSectorAligned(nextRemoteOffset) && buffer.remaining() >= endpoint.getLBADataSize()) {
			int oldLimit = buffer.limit();
			buffer.limit(buffer.position() + floorToSectorSize(buffer.remaining()));
			int toWrite = buffer.remaining();
			middleFuture = endpoint.write(buffer, blockInfo, nextRemoteOffset);
			nextRemoteOffset += toWrite;
			buffer.position(buffer.limit());
			buffer.limit(oldLimit);
		}

		/* end */
		if (buffer.remaining() > 0) {
			endBuffer = endpoint.getStagingBufferCache().get(blockInfo.getAddr() + nextRemoteOffset);
			CrailBuffer stagingBuffer = endBuffer.getBuffer();
			stagingBuffer.position(0);
			stagingBuffer.getByteBuffer().put(buffer.getByteBuffer());
			stagingBuffer.position(0);
			endFuture = endpoint.write(stagingBuffer, blockInfo, nextRemoteOffset);
			endBuffer.setFuture(endFuture);
		}
	}

	@Override
	public boolean isSynchronous() {
		return false;
	}

	@Override
	public boolean cancel(boolean b) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	private static boolean checkIfFutureIsDone(StorageFuture future) {
		return (future != null && future.isDone()) || future == null;
	}

	@Override
	public boolean isDone() {
		if (beginFuture != null && beginFuture.isDone()) {
			if (beginBuffer != null) {
				beginBuffer.put();
				beginBuffer = null;
			}
		}
		if (endFuture != null && endFuture.isDone()) {
			if (endBuffer != null) {
				endBuffer.put();
				endBuffer = null;
			}
		}
		return beginBuffer == null && checkIfFutureIsDone(middleFuture) && endBuffer == null;
	}

	@Override
	public StorageResult get() throws InterruptedException, ExecutionException {
		try {
			return get(2, TimeUnit.MINUTES);
		} catch (TimeoutException e) {
			throw new ExecutionException(e);
		}
	}

	@Override
	public StorageResult get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
		if (!isDone()) {
			long start = System.nanoTime();
			long end = start + TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
			boolean waitTimeOut;
			do {
				waitTimeOut = System.nanoTime() > end;
			} while (!isDone() && !waitTimeOut);
			if (!isDone() && waitTimeOut) {
				throw new TimeoutException("poll wait time out!");
			}
		}
		if (beginFuture != null) {
			beginFuture.get();
		}
		if (middleFuture != null) {
			middleFuture.get();
		}
		if (endFuture != null) {
			endFuture.get();
		}
		return () -> written;
	}
}
