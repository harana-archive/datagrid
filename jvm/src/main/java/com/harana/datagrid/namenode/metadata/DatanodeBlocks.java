package com.harana.datagrid.namenode.metadata;

import com.harana.datagrid.client.namenode.NamenodeErrors;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DatanodeBlocks extends DatanodeInfo {
	private static final Logger logger = LogManager.getLogger();
	
	private final ConcurrentHashMap<Long, BlockInfo> regions;
	private final LinkedBlockingQueue<NameNodeBlockInfo> freeBlocks;
	private long token;
	
	public static DatanodeBlocks fromDatanodeInfo(DatanodeInfo dnInfo) {
		return new DatanodeBlocks(dnInfo.getStorageType(), dnInfo.getStorageClass(), dnInfo.getLocationClass(), dnInfo.getIpAddress(), dnInfo.getPort());
	}	

	private DatanodeBlocks(int storageType, int getStorageClass, int locationClass, byte[] ipAddress, int port) {
		super(storageType, getStorageClass, locationClass, ipAddress, port);
		this.regions = new ConcurrentHashMap<>();
		this.freeBlocks = new LinkedBlockingQueue<>();
	}
	
	public void addFreeBlock(NameNodeBlockInfo nnBlock) {
		regions.put(nnBlock.getRegion().getLba(), nnBlock.getRegion());
		freeBlocks.add(nnBlock);
	}

	public NameNodeBlockInfo getFreeBlock() {
		return this.freeBlocks.poll();
	}
	
	public int getBlockCount() {
		return freeBlocks.size();
	}

	public boolean regionExists(BlockInfo region) {
		return regions.containsKey(region.getLba());
	}

	public short updateRegion(BlockInfo region) {
		BlockInfo oldRegion = regions.get(region.getLba());
		if (oldRegion == null) {
			return NamenodeErrors.ERR_ADD_BLOCK_FAILED;
		} else {
			oldRegion.setBlockInfo(region);
			return 0;
		}
	}

	public void touch() {
		this.token = System.nanoTime() + TimeUnit.SECONDS.toNanos(DatagridConstants.STORAGE_KEEPALIVE * 8);
	}
	
	public boolean isOnline() {
		return System.nanoTime() <= token;
	}	
}