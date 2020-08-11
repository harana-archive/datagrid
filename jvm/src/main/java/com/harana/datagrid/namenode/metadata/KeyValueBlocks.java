package com.harana.datagrid.namenode.metadata;

import com.harana.datagrid.DatagridDataType;

public class KeyValueBlocks extends FileBlocks {
	public KeyValueBlocks(long fd, int fileComponent, DatagridDataType type, int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
	}
}