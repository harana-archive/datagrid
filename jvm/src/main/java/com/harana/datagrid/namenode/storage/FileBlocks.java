package com.harana.datagrid.namenode.storage;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.harana.datagrid.DataType;
import com.harana.datagrid.conf.Constants;

public class FileBlocks extends AbstractNode {
	private final ArrayList<NameNodeBlockInfo> blocks;
	private final ReentrantReadWriteLock lock;
	private final Lock readLock;
	private final Lock writeLock;
	
	public FileBlocks(long fd, int fileComponent, DataType type, int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
		this.blocks = new ArrayList<>(Constants.NAMENODE_FILEBLOCKS);
		this.lock = new ReentrantReadWriteLock();
		this.readLock = lock.readLock();
		this.writeLock = lock.writeLock();
	}

	@Override
	public NameNodeBlockInfo getBlock(int index) {
		readLock.lock();
		try {
			if (index < blocks.size()) {
				return blocks.get(index);
			} else {
				return null;
			}
		} catch(Exception e) {
			return null;
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public boolean addBlock(int index, NameNodeBlockInfo block) {
		writeLock.lock();
		try {
			if (index == blocks.size()) {
				blocks.add(index, block);
				return true;
			} else {
				return false;
			}
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public void freeBlocks(BlockStore blockStore) throws Exception {
		readLock.lock();
		try {
			for (NameNodeBlockInfo blockInfo : blocks) {
				blockStore.addBlock(blockInfo);
			}	
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public AbstractNode putChild(AbstractNode child) throws Exception {
		throw new Exception("Attempt to add a child to a non-container type");
	}

	@Override
	public AbstractNode getChild(int component) throws Exception {
		throw new Exception("Attempto to retrieve child from non-container type");
	}

	@Override
	public AbstractNode removeChild(int component) throws Exception {
		throw new Exception("Attempt to remove child from non-container type");
	}

	@Override
	public void clearChildren(Queue<AbstractNode> queue) throws Exception {
		throw new Exception("Attempt collect children from non-container type");
	}
}