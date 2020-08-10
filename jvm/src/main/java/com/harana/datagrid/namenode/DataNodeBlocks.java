package com.harana.datagrid.namenode;

import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.rpc.RpcErrors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataNodeBlocks extends DataNodeInfo {
	private static final Logger logger = LogManager.getLogger();
	
	private ConcurrentHashMap<Long, BlockInfo> regions;
	private LinkedBlockingQueue<NameNodeBlockInfo> freeBlocks;
	private long token;
	
	public static DataNodeBlocks fromDataNodeInfo(DataNodeInfo dnInfo) {
		return new DataNodeBlocks(dnInfo.getStorageType(), dnInfo.getStorageClass(), dnInfo.getLocationClass(), dnInfo.getIpAddress(), dnInfo.getPort());
	}	

	private DataNodeBlocks(int storageType, int getStorageClass, int locationClass, byte[] ipAddress, int port) {
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
		if (oldRegion == null){
			return RpcErrors.ERR_ADD_BLOCK_FAILED;
		} else {
			oldRegion.setBlockInfo(region);
			return 0;
		}
	}

	public void touch() {
		this.token = System.nanoTime() + TimeUnit.SECONDS.toNanos(CrailConstants.STORAGE_KEEPALIVE * 8);
	}
	
	public boolean isOnline(){
		return System.nanoTime() <= token;
	}	
}