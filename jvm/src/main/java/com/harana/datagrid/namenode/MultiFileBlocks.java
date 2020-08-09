package com.harana.datagrid.namenode;

import com.harana.datagrid.CrailNodeType;

public class MultiFileBlocks extends DirectoryBlocks {

	MultiFileBlocks(long fd, int fileComponent, CrailNodeType type,
			int storageClass, int locationClass, boolean enumerable) {
		super(fd, fileComponent, type, storageClass, locationClass, enumerable);
	}

}
