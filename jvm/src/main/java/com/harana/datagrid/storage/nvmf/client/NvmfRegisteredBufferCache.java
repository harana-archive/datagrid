package com.harana.datagrid.storage.nvmf.client;

import com.harana.datagrid.storage.nvmf.jvnmf.Freeable;
import com.harana.datagrid.storage.nvmf.jvnmf.KeyedNativeBuffer;
import com.harana.datagrid.storage.nvmf.jvnmf.QueuePair;
import com.harana.datagrid.CrailBuffer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class NvmfRegisteredBufferCache implements Freeable {
	private final QueuePair queuePair;
	private final Map<Long, KeyedNativeBuffer> regionMap;
	private boolean valid;

	public NvmfRegisteredBufferCache(QueuePair queuePair) {
		this.queuePair = queuePair;
		this.regionMap = new ConcurrentHashMap<>();
		this.valid = true;
	}

	int getRemoteKey(CrailBuffer buffer) throws IOException {
		CrailBuffer regionBuffer = buffer.getRegion();
		KeyedNativeBuffer keyedNativeBuffer = regionMap.get(regionBuffer.address());
		if (keyedNativeBuffer == null) {
			/* region has not been registered yet */
			keyedNativeBuffer = queuePair.registerMemory(regionBuffer.getByteBuffer());
			KeyedNativeBuffer prevKeyedNativeBuffer =
					regionMap.putIfAbsent(keyedNativeBuffer.getAddress(), keyedNativeBuffer);
			if (prevKeyedNativeBuffer != null) {
				/* someone registered the same region in parallel */
				keyedNativeBuffer.free();
				keyedNativeBuffer = prevKeyedNativeBuffer;
			}
		}
		return keyedNativeBuffer.getRemoteKey();
	}


	@Override
	public void free() throws IOException {
		for (KeyedNativeBuffer buffer : regionMap.values()) {
			buffer.free();
		}
		valid = false;
	}

	@Override
	public boolean isValid() {
		return valid;
	}
}
