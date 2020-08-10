package com.harana.datagrid.namenode.storage;

import com.harana.datagrid.DataType;

public class MultiFileBlocks extends DirectoryBlocks {

	MultiFileBlocks(long fd, int fileComponent, DataType type, int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
	}
}