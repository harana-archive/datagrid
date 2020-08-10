package com.harana.datagrid.namenode;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.rpc.RpcErrors;
import com.harana.datagrid.utils.AtomicIntegerModulo;
import com.harana.datagrid.utils.CrailUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BlockStore {
	private static final Logger logger = LogManager.getLogger();
	
	private StorageClass[] storageClasses;
	
	public BlockStore(){
		storageClasses = new StorageClass[CrailConstants.STORAGE_CLASSES]; 
		for (int i = 0; i < CrailConstants.STORAGE_CLASSES; i++){
			this.storageClasses[i] = new StorageClass(i);
		}		
	}

	public short addBlock(NameNodeBlockInfo blockInfo) throws UnknownHostException {
		int storageClass = blockInfo.getDnInfo().getStorageClass();
		return storageClasses[storageClass].addBlock(blockInfo);
	}

	public boolean regionExists(BlockInfo region) {
		int storageClass = region.getDnInfo().getStorageClass();
		return storageClasses[storageClass].regionExists(region);
	}

	public short updateRegion(BlockInfo region) {
		int storageClass = region.getDnInfo().getStorageClass();
		return storageClasses[storageClass].updateRegion(region);
	}

	public NameNodeBlockInfo getBlock(int storageClass, int locationAffinity) throws InterruptedException {
		NameNodeBlockInfo block = null;
		if (storageClass > 0){
			if (storageClass < storageClasses.length){
				block = storageClasses[storageClass].getBlock(locationAffinity);
			} else {
				//TODO: warn if requested storage class is invalid
			}
		}
		if (block == null){
			for (int i = 0; i < storageClasses.length; i++){
				block = storageClasses[i].getBlock(locationAffinity);
				if (block != null){
					break;
				}
			}
		}
		
		return block;
	}

	public DataNodeBlocks getDataNode(DataNodeInfo dnInfo) {
		int storageClass = dnInfo.getStorageClass();
		return storageClasses[storageClass].getDataNode(dnInfo);
	}
}