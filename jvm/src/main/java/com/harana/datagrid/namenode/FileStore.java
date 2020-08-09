package com.harana.datagrid.namenode;

import java.io.IOException;

import com.harana.datagrid.CrailNodeType;
import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.metadata.FileName;
import com.harana.datagrid.rpc.RpcErrors;
import com.harana.datagrid.rpc.RpcNameNodeState;

public class FileStore {
	private Sequencer sequencer;
	private AbstractNode root;
	
	public FileStore(Sequencer sequencer) throws IOException { 
		this.sequencer = sequencer;
		this.root = createNode(new FileName("/").getFileComponent(), CrailNodeType.DIRECTORY, CrailConstants.STORAGE_ROOTCLASS, 0, false);
	}
	
	public AbstractNode createNode(int fileComponent, CrailNodeType type, int storageClass, int locationClass, boolean enumerable) throws IOException {
		if (type == CrailNodeType.DIRECTORY){
			return new DirectoryBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == CrailNodeType.MULTIFILE){
			return new MultiFileBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == CrailNodeType.TABLE){
			return new TableBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == CrailNodeType.KEYVALUE){
			return new KeyValueBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else if (type == CrailNodeType.DATAFILE){
			return new FileBlocks(sequencer.getNextId(), fileComponent, type, storageClass, locationClass, enumerable);
		} else {
			throw new IOException("File type unkown: " + type);
		}
	}	
	
	public AbstractNode retrieveFile(FileName filename, RpcNameNodeState error) throws Exception{
		return retrieveFileInternal(filename, filename.getLength(), error);
	}
	
	public AbstractNode retrieveParent(FileName filename, RpcNameNodeState error) throws Exception{
		return retrieveFileInternal(filename, filename.getLength()-1, error);
	}	
	
	public AbstractNode getRoot() {
		return root;
	}	
	
	public void dump(){
		root.dump();
	}
	
	private AbstractNode retrieveFileInternal(FileName filename, int length, RpcNameNodeState error) throws Exception {
		if (length >= CrailConstants.DIRECTORY_DEPTH){
			error.setError(RpcErrors.ERR_FILE_COMPONENTS_EXCEEDED);
			return null;
		}
		
		AbstractNode current = root;
		for (int i = 0; i < length; i++){
			int component = filename.getComponent(i);
			current = current.getChild(component);
			if (current == null){
				break;
			}
		}
		
		return current;
	}
}
