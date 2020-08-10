package com.harana.datagrid.datanode.nvmf.client;

import com.harana.datagrid.Buffer;
import com.harana.datagrid.BufferCache;
import com.harana.datagrid.conf.Configuration;
import com.harana.datagrid.conf.Constants;
import com.harana.datagrid.memory.MappedBufferCache;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class NvmfStagingBufferCacheTest {

	@BeforeClass
	public static void init() throws IOException {
		Constants.updateConstants(Configuration.createConfigurationFromFile());
	}

	private static BufferCache bufferCache;
	static BufferCache getBufferCache() throws IOException {
		if (bufferCache == null) {
			bufferCache = new MappedBufferCache();
		}
		return bufferCache;
	}


	@Test(expected = IllegalArgumentException.class)
	public void createBufferCache() throws IOException {
		new NvmfStagingBufferCache(getBufferCache(), -1, 512);
		new NvmfStagingBufferCache(getBufferCache(),0, 512);
		new NvmfStagingBufferCache(getBufferCache(),1024, -1);
		new NvmfStagingBufferCache(getBufferCache(),1024, 0);
	}

	@Test(expected = OutOfMemoryError.class)
	public void outOfMemory() throws Exception {
		NvmfStagingBufferCache bufferCache = new NvmfStagingBufferCache(getBufferCache(),1, 512);
		NvmfStagingBufferCache.BufferCacheEntry bufferCacheEntry = bufferCache.get(0);
		NvmfStagingBufferCache.BufferCacheEntry bufferCacheEntry2 = bufferCache.get(1);
	}

	@Test
	public void bufferExists() throws Exception {
		NvmfStagingBufferCache bufferCache = new NvmfStagingBufferCache(getBufferCache(),1, 512);
		NvmfStagingBufferCache.BufferCacheEntry bufferCacheEntry = bufferCache.get(0);
		NvmfStagingBufferCache.BufferCacheEntry existingBufferCacheEntry = bufferCache.getExisting(0);
		assertEquals(bufferCacheEntry, existingBufferCacheEntry);
	}

	@Test
	public void recycleBuffers() throws Exception {
		NvmfStagingBufferCache.BufferCacheEntry[] bufferCacheEntry = new NvmfStagingBufferCache.BufferCacheEntry[5];
		Set<Buffer> buffers = new HashSet<>();
		NvmfStagingBufferCache bufferCache = new NvmfStagingBufferCache(getBufferCache(), bufferCacheEntry.length, 512);
		for (int i = 0; i < bufferCacheEntry.length; i++) {
			bufferCacheEntry[i] = bufferCache.get(i);
			buffers.add(bufferCacheEntry[i].getBuffer());
			bufferCacheEntry[i].put();
		}
		for (int i = 0; i < bufferCacheEntry.length; i++) {
			bufferCacheEntry[i] = bufferCache.get(i + bufferCacheEntry.length);
			assertTrue(buffers.remove(bufferCacheEntry[i].getBuffer()));
		}
	}
}
