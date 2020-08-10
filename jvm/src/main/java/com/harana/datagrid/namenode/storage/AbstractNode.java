package com.harana.datagrid.namenode.storage;

import java.util.Queue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import com.harana.datagrid.DataType;
import com.harana.datagrid.metadata.FileInfo;

public abstract class AbstractNode extends FileInfo implements Delayed {
	private final int storageClass;
	private final int locationClass;

	private int fileComponent;
	private long delay;

	// children manipulation
	// adds or replaces a child, returns previous value or null if there was no mapping
	public abstract AbstractNode putChild(AbstractNode child) throws Exception;
	// get the child with the given component name, returns null if there is no mapping
	public abstract AbstractNode getChild(int component) throws Exception;
	// remove a child, returns previous value of existing or null otherwise
	public abstract AbstractNode removeChild(int component) throws Exception;
	// clear all the children (used by GC)
	public abstract void clearChildren(Queue<AbstractNode> queue) throws Exception;
	// public abstract AbstractNode updateParent() throws Exception;
	
	// block manipulation
	// adds a new block at a given index, returns true if succesful, false otherwise
	public abstract boolean addBlock(int index, NameNodeBlockInfo block) throws Exception;
	// get block at the given index, returns a valid block or null otherwise
	public abstract NameNodeBlockInfo getBlock(int index) throws Exception;
	// clear all the blocks (used by GC)
	public abstract void freeBlocks(BlockStore blockStore) throws Exception;	
	
	public AbstractNode(long fd, int fileComponent, DataType type, int storageClass, int locationAffinity, boolean enumerable) {
		super(fd, type, enumerable);
		this.fileComponent = fileComponent;
		this.storageClass = storageClass;
		this.locationClass = locationAffinity;
		this.delay = System.currentTimeMillis();
		this.setModificationTime(System.currentTimeMillis());
	}
	
	void rename(int newFileComponent) {
		this.fileComponent = newFileComponent;
	}	

	public int getComponent() {
		return this.fileComponent;
	}
	
	public void dump() {
		System.out.println(this.toString());
	}
	
	@Override
	protected void setDirOffset(long dirOffset) {
		super.setDirOffset(dirOffset);
	}	
	
	@Override
	public String toString() {
		return String.format("%08d\t%08d\t\t%08d\t\t%08d\t\t%08d", getFd(), fileComponent, getCapacity(), getType().getLabel(), getDirOffset());
	}	

	@Override
	public long getDelay(TimeUnit unit) {
		long diff = delay - System.currentTimeMillis();
		return unit.convert(diff, TimeUnit.MILLISECONDS);
	}

	public void setDelay(long delay) {
		this.delay = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(delay);
	}

	@Override
	public int compareTo(Delayed o) {
		return 0;
	}

	public int getStorageClass() {
		return storageClass;
	}

	public int getLocationClass() {
		return locationClass;
	}
}
