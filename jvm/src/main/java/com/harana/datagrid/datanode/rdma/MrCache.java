package com.harana.datagrid.datanode.rdma;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.harana.datagrid.DatagridBuffer;

import com.harana.datagrid.rdma.verbs.IbvMr;
import com.harana.datagrid.rdma.verbs.IbvPd;

public class MrCache {
	private final ConcurrentHashMap<Integer, DeviceMrCache> cache;
	private final AtomicLong cacheOps;
	private final AtomicLong cacheMisses;
	
	public MrCache() {
		this.cache = new ConcurrentHashMap<>();
		this.cacheMisses = new AtomicLong(0);
		this.cacheOps = new AtomicLong(0);
	}
	
	public DeviceMrCache getDeviceCache(IbvPd pd) throws IOException {
		DeviceMrCache deviceCache = cache.get(pd.getHandle());
		if (deviceCache == null) {
			deviceCache = new DeviceMrCache(pd);
			DeviceMrCache oldLine = cache.putIfAbsent(pd.getHandle(), deviceCache);
			if (oldLine != null) {
				deviceCache = oldLine;
			}
		}
		return deviceCache;
	}
	
	public void close() throws IOException {
		for (DeviceMrCache line : cache.values()) {
			line.close();
		}
		cache.clear();			
	}
	
	public long ops() {
		return cacheOps.get();
	}
	
	public long missed() {
		return cacheMisses.get();
	}	
	
	public void reset() {
		this.cacheOps.set(0);
		this.cacheMisses.set(0);
	}
	
	public static class DeviceMrCache {
		private final IbvPd pd;
		private final ConcurrentHashMap<Long, IbvMr> device;
		
		public DeviceMrCache(IbvPd pd) {
			this.pd = pd;
			this.device = new ConcurrentHashMap<>();
		}
		
		public IbvMr get(DatagridBuffer buffer) throws IOException{
			return device.get(buffer.address());
		}
		
		public void put(IbvMr mr) {
			device.put(mr.getAddr(), mr);
		}		
		
		public void close() throws IOException {
			for (IbvMr mr : device.values()) {
				mr.deregMr().free();
			}
			device.clear();
		}

		public IbvPd getPd() {
			return pd;
		}
	}
}