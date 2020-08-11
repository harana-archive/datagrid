package com.harana.datagrid.namenode.metadata;

import com.harana.datagrid.DatagridStorageClass;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BlockStore {
	private static final Logger logger = LogManager.getLogger();
	
	private final StorageClass[] storageClasses;
	
	public BlockStore() {
		storageClasses = new StorageClass[DatagridConstants.STORAGE_CLASSES];
		for (int i = 0; i < DatagridConstants.STORAGE_CLASSES; i++) {
			this.storageClasses[i] = new StorageClass(i);
		}		
	}

	public short addBlock(NameNodeBlockInfo blockInfo) {
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

	public NameNodeBlockInfo getBlock(int storageClass, int locationAffinity) {
		NameNodeBlockInfo block = null;
		if (storageClass > 0) {
			if (storageClass < storageClasses.length) {
				block = storageClasses[storageClass].getBlock(locationAffinity);
			} else {
				//TODO: warn if requested storage class is invalid
			}
		}
		if (block == null) {
			for (StorageClass aClass : storageClasses) {
				block = aClass.getBlock(locationAffinity);
				if (block != null) {
					break;
				}
			}
		}
		
		return block;
	}

	public DatanodeBlocks getDataNode(DatanodeInfo dnInfo) {
		int storageClass = dnInfo.getStorageClass();
		return storageClasses[storageClass].getDataNode(dnInfo);
	}
}