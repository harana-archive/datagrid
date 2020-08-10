package com.harana.datagrid.namenode.storage;

import com.harana.datagrid.DataType;
import com.harana.datagrid.conf.Constants;

public class TableBlocks extends DirectoryBlocks {

	TableBlocks(long fd, int fileComponent, DataType type, int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
	}

	@Override
	public AbstractNode putChild(AbstractNode child) throws Exception {
		if (!child.getType().isKeyValue()) {
			throw new Exception("Attempt to create key/value pair in container other than a table");
		}
		
		AbstractNode oldNode = children.put(child.getComponent(), child);
		if (child.isEnumerable()) {
			child.setDirOffset(dirOffsetCounter.getAndAdd(Constants.DIRECTORY_RECORD));
		}		
		return oldNode;
	}
}
