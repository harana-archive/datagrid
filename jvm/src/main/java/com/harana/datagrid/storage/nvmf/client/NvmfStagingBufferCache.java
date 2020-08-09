package com.harana.datagrid.storage.nvmf.client;

import com.harana.datagrid.CrailBuffer;
import com.harana.datagrid.CrailBufferCache;
import com.harana.datagrid.storage.StorageFuture;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NvmfStagingBufferCache {
	private final Map<Long, BufferCacheEntry> remoteAddressMap;
	private final Queue<CrailBuffer> freeBuffers;
	private int buffersLeft;
	private final int lbaDataSize;
	private final CrailBufferCache bufferCache;

	private final CrailBufferCache getBufferCache() {
		return bufferCache;
	}

	NvmfStagingBufferCache(CrailBufferCache bufferCache, int maxEntries, int lbaDataSize) {
		if (maxEntries <= 0) {
			throw new IllegalArgumentException("maximum entries (" + maxEntries + ") <= 0");
		}
		if (lbaDataSize <= 0) {
			throw new IllegalArgumentException("LBA data size (" + lbaDataSize + ") <= 0");
		}
		this.remoteAddressMap = new ConcurrentHashMap<>(maxEntries);
		this.freeBuffers = new ArrayBlockingQueue<>(maxEntries);
		this.buffersLeft = maxEntries;
		this.lbaDataSize = lbaDataSize;
		this.bufferCache = bufferCache;
	}

	synchronized void allocateFreeBuffers() throws Exception {
		if (!freeBuffers.isEmpty()) {
			return;
		}
		if (buffersLeft == 0) {
			/* TODO: make sure this happens rarely */
			Iterator<BufferCacheEntry> iterator = remoteAddressMap.values().iterator();
			while (iterator.hasNext()) {
				BufferCacheEntry currentEntry = iterator.next();
				if (currentEntry.tryFree()) {
					iterator.remove();
					freeBuffers.add(currentEntry.getBuffer());
					return;
				}
			}
			throw new OutOfMemoryError();
		}

		CrailBuffer buffer = getBufferCache().allocateBuffer();
		if (buffer == null) {
			throw new OutOfMemoryError();
		}
		if (buffer.capacity() < lbaDataSize) {
			throw new IllegalArgumentException("Slice size (" + buffer.capacity() + ") smaller LBA data size (" +
					lbaDataSize + ")");
		}
		int numStagingBuffers = buffer.remaining() / lbaDataSize;
		while (numStagingBuffers-- > 0 && buffersLeft > 0) {
			buffer.limit(buffer.position() + lbaDataSize);
			freeBuffers.add(buffer.slice());
			buffer.position(buffer.limit());
			buffersLeft--;
		}
	}

	static class BufferCacheEntry {
		private final CrailBuffer buffer;
		private final AtomicInteger pending;
		private StorageFuture future;

		BufferCacheEntry(CrailBuffer buffer) {
			this.buffer = buffer;
			this.pending = new AtomicInteger(1);
		}

		public StorageFuture getFuture() {
			return future;
		}

		public void setFuture(StorageFuture future) {
			this.future = future;
		}

		void put() {
			pending.decrementAndGet();
		}

		boolean tryGet() {
			int prevPending;
			do {
				prevPending = pending.get();
				if (prevPending < 0) {
					return false;
				}
			} while (!pending.compareAndSet(prevPending, prevPending + 1));
			return true;
		}

		boolean tryFree() {
			return pending.compareAndSet(0, -1);
		}

		CrailBuffer getBuffer() {
			return buffer;
		}


	}

	BufferCacheEntry get(long alignedRemoteAddress) throws Exception {
		CrailBuffer buffer;
		do {
			buffer = freeBuffers.poll();
			if (buffer == null) {
				allocateFreeBuffers();
			}
		} while (buffer == null);
		buffer.clear();

		BufferCacheEntry entry = new BufferCacheEntry(buffer);
		BufferCacheEntry prevEntry = remoteAddressMap.putIfAbsent(alignedRemoteAddress, entry);
		if (prevEntry != null) {
			if (prevEntry.tryFree()) {
				freeBuffers.add(prevEntry.getBuffer());
			} /*else {
				we lost the race with allocateFreeBuffers which freed the buffer
			}*/
		}
		return entry;
	}

	BufferCacheEntry getExisting(long alignedRemoteAddress) {
		BufferCacheEntry entry = remoteAddressMap.get(alignedRemoteAddress);
		if (entry != null && !entry.tryGet()) {
			entry = null;
		}
		return entry;
	}
}
