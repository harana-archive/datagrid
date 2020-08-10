package com.harana.datagrid.namenode;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.harana.datagrid.CrailNodeType;
import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.metadata.BlockInfo;

public class DirectoryBlocks extends AbstractNode {
	protected AtomicLong dirOffsetCounter;
	protected ConcurrentHashMap<Integer, AbstractNode> children;	
	private ConcurrentHashMap<Integer, NameNodeBlockInfo> blocks;
	
	DirectoryBlocks(long fd, int fileComponent, CrailNodeType type, int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
		this.children = new ConcurrentHashMap<>();
		this.dirOffsetCounter = new AtomicLong(0);
		this.blocks = new ConcurrentHashMap<>();
	}
	
	public AbstractNode putChild(AbstractNode child) throws Exception {
		AbstractNode old = children.putIfAbsent(child.getComponent(), child);
		if (old != null){
			throw new Exception("File exists");
		}
		if (child.isEnumerable()) {
			child.setDirOffset(dirOffsetCounter.getAndAdd(CrailConstants.DIRECTORY_RECORD));
		}
		return old;
	}	
	
	public AbstractNode getChild(int component) {
		return children.get(component);
	}	
	
	public AbstractNode removeChild(int component) {
		return children.remove(component);
	}
	
	@Override
	public NameNodeBlockInfo getBlock(int index) {
		return blocks.get(index);
	}

	@Override
	public boolean addBlock(int index, NameNodeBlockInfo block) {
		BlockInfo old = blocks.putIfAbsent(index, block);
		return old == null;
	}

	@Override
	public void freeBlocks(BlockStore blockStore) throws Exception {
		for (NameNodeBlockInfo blockInfo : blocks.values()) {
			blockStore.addBlock(blockInfo);
		}	
	}

	@Override
	public long setCapacity(long newcapacity) {
		return this.getCapacity();
	}

	@Override
	public void updateToken() {
	}

	@Override
	public void clearChildren(Queue<AbstractNode> queue) {
		queue.addAll(children.values());
	}

	@Override
	public void dump() {
		super.dump();
		for (AbstractNode child : children.values()){
			child.dump();
		}		
	}
}
