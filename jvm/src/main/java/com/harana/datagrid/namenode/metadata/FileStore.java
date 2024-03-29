package com.harana.datagrid.namenode.metadata;

import java.io.IOException;

import com.harana.datagrid.DatagridDataType;
import com.harana.datagrid.client.namenode.NamenodeErrors;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.metadata.FileName;
import com.harana.datagrid.namenode.NamenodeState;

public class FileStore {
	private final Sequencer sequencer;
	private final AbstractNode root;
	
	public FileStore(Sequencer sequencer) throws IOException { 
		this.sequencer = sequencer;
		this.root = createNode(new FileName("/").getFileComponent(), DatagridDataType.DIRECTORY, DatagridConstants.STORAGE_ROOTCLASS, 0, false);
	}
	
	public AbstractNode createNode(int fileComponent, DatagridDataType type, int storageClass, int locationClass, boolean enumerable) throws IOException {
		if (type == DatagridDataType.DIRECTORY) {
			return new DirectoryBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == DatagridDataType.MULTIFILE) {
			return new MultiFileBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == DatagridDataType.TABLE) {
			return new TableBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == DatagridDataType.KEYVALUE) {
			return new KeyValueBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == DatagridDataType.DATAFILE) {
			return new FileBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else {
			throw new IOException("File type unkown: " + type);
		}
	}	
	
	public AbstractNode retrieveFile(FileName filename, NamenodeState error) throws Exception{
		return retrieveFileInternal(filename, filename.getLength(), error);
	}
	
	public AbstractNode retrieveParent(FileName filename, NamenodeState error) throws Exception{
		return retrieveFileInternal(filename, filename.getLength()-1, error);
	}	
	
	public AbstractNode getRoot() {
		return root;
	}	
	
	public void dump() {
		root.dump();
	}
	
	private AbstractNode retrieveFileInternal(FileName filename, int length, NamenodeState error) throws Exception {
		if (length >= DatagridConstants.DIRECTORY_DEPTH) {
			error.setError(NamenodeErrors.ERR_FILE_COMPONENTS_EXCEEDED);
			return null;
		}
		
		AbstractNode current = root;
		for (int i = 0; i < length; i++) {
			int component = filename.getComponent(i);
			current = current.getChild(component);
			if (current == null) {
				break;
			}
		}
		return current;
	}
}