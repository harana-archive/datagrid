package com.harana.datagrid.namenode.metadata;

import com.harana.datagrid.DatagridDataType;

public class MultiFileBlocks extends DirectoryBlocks {

	MultiFileBlocks(long fd, int fileComponent, DatagridDataType type, int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
	}
}