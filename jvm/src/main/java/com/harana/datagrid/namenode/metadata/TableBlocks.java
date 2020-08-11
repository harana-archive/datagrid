package com.harana.datagrid.namenode.metadata;

import com.harana.datagrid.DatagridDataType;
import com.harana.datagrid.conf.DatagridConstants;

public class TableBlocks extends DirectoryBlocks {

	TableBlocks(long fd, int fileComponent, DatagridDataType type, int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
	}

	@Override
	public AbstractNode putChild(AbstractNode child) throws Exception {
		if (!child.getType().isKeyValue()) {
			throw new Exception("Attempt to create key/value pair in container other than a table");
		}
		
		AbstractNode oldNode = children.put(child.getComponent(), child);
		if (child.isEnumerable()) {
			child.setDirOffset(dirOffsetCounter.getAndAdd(DatagridConstants.DIRECTORY_RECORD));
		}		
		return oldNode;
	}
}
