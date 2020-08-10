package com.harana.datagrid.namenode.storage;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.harana.datagrid.DataType;
import com.harana.datagrid.client.namenode.NamenodeErrors;
import com.harana.datagrid.conf.Constants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.metadata.FileInfo;
import com.harana.datagrid.metadata.FileName;
import com.harana.datagrid.namenode.RpcNameNodeService;
import com.harana.datagrid.namenode.NamenodeState;
import com.harana.datagrid.namenode.NamenodeProtocol;
import com.harana.datagrid.namenode.NamenodeRequest;
import com.harana.datagrid.namenode.NamenodeResponse;
import com.harana.datagrid.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NameNodeService implements RpcNameNodeService, Sequencer {
	private static final Logger logger = LogManager.getLogger();
	
	private final long serviceId;
	private final long serviceSize;
	private final AtomicLong sequenceId;
	private final BlockStore blockStore;
	private final DelayQueue<AbstractNode> deleteQueue;
	private final FileStore fileTree;
	private final ConcurrentHashMap<Long, AbstractNode> fileTable;

	public NameNodeService() throws IOException {
		URI uri = URI.create(Constants.NAMENODE_ADDRESS);
		String query = uri.getRawQuery();
		StringTokenizer tokenizer = new StringTokenizer(query, "&");
		this.serviceId = Long.parseLong(tokenizer.nextToken().substring(3));
		this.serviceSize = Long.parseLong(tokenizer.nextToken().substring(5));
		this.sequenceId = new AtomicLong(serviceId);
		this.blockStore = new BlockStore();
		this.deleteQueue = new DelayQueue<>();
		this.fileTree = new FileStore(this);
		this.fileTable = new ConcurrentHashMap<>();
		GCServer gcServer = new GCServer(this, deleteQueue);
		
		AbstractNode root = fileTree.getRoot();
		fileTable.put(root.getFd(), root);
		Thread gc = new Thread(gcServer);
		gc.start();				
	}
	
	public long getNextId() {
		return sequenceId.getAndAdd(serviceSize);
	}

	@Override
	public short createFile(NamenodeRequest.CreateFile request, NamenodeResponse.CreateFile response, NamenodeState errorState) throws Exception {
		//check protocol
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_CREATE_FILE, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}

		//get params
		FileName fileHash = request.getFileName();
		DataType type = request.getFileType();
		boolean writeable = !type.isDirectory();
		int storageClass = request.getStorageClass();
		int locationClass = request.getLocationClass();
		boolean enumerable = request.isEnumerable();
		
		//check params
		if (type.isContainer() && locationClass > 0) {
			return NamenodeErrors.ERR_DIR_LOCATION_AFFINITY_MISMATCH;
		}
		
		//rpc
		AbstractNode parentInfo = fileTree.retrieveParent(fileHash, errorState);
		if (errorState.getError() != NamenodeErrors.ERR_OK) {
			return errorState.getError();
		}		
		if (parentInfo == null) {
			return NamenodeErrors.ERR_PARENT_MISSING;
		} 	
		if (!parentInfo.getType().isContainer()) {
			return NamenodeErrors.ERR_PARENT_NOT_DIR;
		}
		
		if (storageClass < 0) {
			storageClass = parentInfo.getStorageClass();
		}
		if (locationClass < 0) {
			locationClass = parentInfo.getLocationClass();
		}
		
		AbstractNode fileInfo = fileTree.createNode(fileHash.getFileComponent(), type, storageClass, locationClass, enumerable);
		try {
			AbstractNode oldNode = parentInfo.putChild(fileInfo);
			if (oldNode != null && oldNode.getFd() != fileInfo.getFd()) {
				appendToDeleteQueue(oldNode);				
			}		
		} catch(Exception e) {
			return NamenodeErrors.ERR_FILE_EXISTS;
		}
		fileTable.put(fileInfo.getFd(), fileInfo);
		
		NameNodeBlockInfo fileBlock = blockStore.getBlock(fileInfo.getStorageClass(), fileInfo.getLocationClass());
		if (fileBlock == null) {
			return NamenodeErrors.ERR_NO_FREE_BLOCKS;
		}			
		if (!fileInfo.addBlock(0, fileBlock)) {
			return NamenodeErrors.ERR_ADD_BLOCK_FAILED;
		}
		
		NameNodeBlockInfo parentBlock = null;
		if (fileInfo.getDirOffset() >= 0) {
			int index = Utils.computeIndex(fileInfo.getDirOffset());
			parentBlock = parentInfo.getBlock(index);
			if (parentBlock == null) {
				parentBlock = blockStore.getBlock(parentInfo.getStorageClass(), parentInfo.getLocationClass());
				if (parentBlock == null) {
					return NamenodeErrors.ERR_NO_FREE_BLOCKS;
				}			
				if (!parentInfo.addBlock(index, parentBlock)) {
					blockStore.addBlock(parentBlock);
					parentBlock = parentInfo.getBlock(index);
					if (parentBlock == null) {
						blockStore.addBlock(fileBlock);
						return NamenodeErrors.ERR_CREATE_FILE_FAILED;
					}
				}
			}
			parentInfo.incCapacity(Constants.DIRECTORY_RECORD);
		}
		
		if (writeable) {
			fileInfo.updateToken();
			response.shipToken(true);
		} else {
			response.shipToken(false);
		}
		response.setParentInfo(parentInfo);
		response.setFileInfo(fileInfo);
		response.setFileBlock(fileBlock);
		response.setDirBlock(parentBlock);
		
		if (Constants.DEBUG) {
			logger.info("createFile: fd " + fileInfo.getFd() + ", parent " + parentInfo.getFd() + ", writeable " + writeable + ", token " + fileInfo.getToken() + ", capacity " + fileInfo.getCapacity() + ", dirOffset " + fileInfo.getDirOffset());
		}	
		
		return NamenodeErrors.ERR_OK;
	}	
	
	@Override
	public short getFile(NamenodeRequest.GetFile request, NamenodeResponse.GetFile response, NamenodeState errorState) throws Exception {
		//check protocol
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_GET_FILE, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		FileName fileHash = request.getFileName();
		boolean writeable = request.isWriteable();

		//rpc
		AbstractNode fileInfo = fileTree.retrieveFile(fileHash, errorState);
		if (errorState.getError() != NamenodeErrors.ERR_OK) {
			return errorState.getError();
		}		
		if (fileInfo == null) {
			return NamenodeErrors.ERR_GET_FILE_FAILED;
		}
		if (writeable && !fileInfo.tokenFree()) {
			return NamenodeErrors.ERR_TOKEN_TAKEN;
		} 
		
		if (writeable) {
			fileInfo.updateToken();
		}
		fileTable.put(fileInfo.getFd(), fileInfo);
		
		BlockInfo fileBlock = fileInfo.getBlock(0);
		
		response.setFileInfo(fileInfo);
		response.setFileBlock(fileBlock);
		if (writeable) {
			response.shipToken();
		}
		
		if (Constants.DEBUG) {
			logger.info("getFile: fd " + fileInfo.getFd() + ", isDir " + fileInfo.getType().isDirectory() + ", token " + fileInfo.getToken() + ", capacity " + fileInfo.getCapacity());
		}			
		
		return NamenodeErrors.ERR_OK;
	}
	
	@Override
	public short setFile(NamenodeRequest.SetFile request, NamenodeResponse.Void response, NamenodeState errorState) throws Exception {
		//check protocol
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_SET_FILE, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		FileInfo fileInfo = request.getFileInfo();
		boolean close = request.isClose();

		//rpc
		AbstractNode storedFile = fileTable.get(fileInfo.getFd());
		if (storedFile == null) {
			return NamenodeErrors.ERR_FILE_NOT_OPEN;
		}
		
		if (storedFile.getToken() > 0 && storedFile.getToken() == fileInfo.getToken()) {
			storedFile.setCapacity(fileInfo.getCapacity());	
		}		
		if (close) {
			storedFile.resetToken();
		}
		
		if (Constants.DEBUG) {
			logger.info("setFile: " + fileInfo.toString() + ", close " + close);
		}
		
		return NamenodeErrors.ERR_OK;
	}

	@Override
	public short removeFile(NamenodeRequest.RemoveFile request, NamenodeResponse.DeleteFile response, NamenodeState errorState) throws Exception {
		//check protocol
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_REMOVE_FILE, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		FileName fileHash = request.getFileName();
		
		//rpc
		AbstractNode parentInfo = fileTree.retrieveParent(fileHash, errorState);
		if (errorState.getError() != NamenodeErrors.ERR_OK) {
			return errorState.getError();
		}		
		if (parentInfo == null) {
			return NamenodeErrors.ERR_CREATE_FILE_FAILED;
		} 		
		
		AbstractNode fileInfo = fileTree.retrieveFile(fileHash, errorState);
		if (errorState.getError() != NamenodeErrors.ERR_OK) {
			return errorState.getError();
		}		
		if (fileInfo == null) {
			return NamenodeErrors.ERR_GET_FILE_FAILED;
		}	
		
		response.setParentInfo(parentInfo);
		response.setFileInfo(fileInfo);
		
		fileInfo = parentInfo.removeChild(fileInfo.getComponent());
		if (fileInfo == null) {
			return NamenodeErrors.ERR_GET_FILE_FAILED;
		}
		
		appendToDeleteQueue(fileInfo);
		
		if (Constants.DEBUG) {
			logger.info("removeFile: filename, fd " + fileInfo.getFd());
		}	
		
		return NamenodeErrors.ERR_OK;
	}	
	
	@Override
	public short renameFile(NamenodeRequest.RenameFile request, NamenodeResponse.RenameFile response, NamenodeState errorState) throws Exception {
		//check protocol
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_RENAME_FILE, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}	
		
		//get params
		FileName srcFileHash = request.getSrcFileName();
		FileName dstFileHash = request.getDstFileName();
		
		//rpc
		AbstractNode srcParent = fileTree.retrieveParent(srcFileHash, errorState);
		if (errorState.getError() != NamenodeErrors.ERR_OK) {
			return errorState.getError();
		}		
		if (srcParent == null) {
			return NamenodeErrors.ERR_GET_FILE_FAILED;
		} 		
		
		AbstractNode srcFile = fileTree.retrieveFile(srcFileHash, errorState);
		if (errorState.getError() != NamenodeErrors.ERR_OK) {
			return errorState.getError();
		}		
		if (srcFile == null) {
			return NamenodeErrors.ERR_SRC_FILE_NOT_FOUND;
		}
		
		//directory block
		int index = Utils.computeIndex(srcFile.getDirOffset());
		NameNodeBlockInfo srcBlock = srcParent.getBlock(index);
		if (srcBlock == null) {
			return NamenodeErrors.ERR_GET_FILE_FAILED;
		}
		//end
		
		response.setSrcParent(srcParent);
		response.setSrcFile(srcFile);
		response.setSrcBlock(srcBlock);
		
		AbstractNode dstParent = fileTree.retrieveParent(dstFileHash, errorState);
		if (errorState.getError() != NamenodeErrors.ERR_OK) {
			return errorState.getError();
		}		
		if (dstParent == null) {
			return NamenodeErrors.ERR_GET_FILE_FAILED;
		} 
		
		AbstractNode dstFile = fileTree.retrieveFile(dstFileHash, errorState);
		if (dstFile != null && !dstFile.getType().isDirectory()) {
			return NamenodeErrors.ERR_FILE_EXISTS;
		}		
		if (dstFile != null && dstFile.getType().isDirectory()) {
			dstParent = dstFile;
		} 
		
		srcFile = srcParent.removeChild(srcFile.getComponent());
		if (srcFile == null) {
			return NamenodeErrors.ERR_SRC_FILE_NOT_FOUND;
		}
		srcFile.rename(dstFileHash.getFileComponent());
		try {
			AbstractNode oldNode = dstParent.putChild(srcFile);
			if (oldNode != null && oldNode.getFd() != srcFile.getFd()) {
				appendToDeleteQueue(oldNode);				
			}				
			dstFile = srcFile;
		} catch(Exception e) {
			return NamenodeErrors.ERR_FILE_EXISTS;
		}
		
		//directory block
		index = Utils.computeIndex(srcFile.getDirOffset());
		NameNodeBlockInfo dstBlock = dstParent.getBlock(index);
		if (dstBlock == null) {
			dstBlock = blockStore.getBlock(dstParent.getStorageClass(), dstParent.getLocationClass());
			if (dstBlock == null) {
				return NamenodeErrors.ERR_NO_FREE_BLOCKS;
			}			
			if (!dstParent.addBlock(index, dstBlock)) {
				blockStore.addBlock(dstBlock);
				dstBlock = dstParent.getBlock(index);
				if (dstBlock == null) {
					blockStore.addBlock(srcBlock);
					return NamenodeErrors.ERR_CREATE_FILE_FAILED;
				}
			} 
		}
		dstParent.incCapacity(Constants.DIRECTORY_RECORD);
		//end
		
		response.setDstParent(dstParent);
		response.setDstFile(dstFile);
		response.setDstBlock(dstBlock);
		
		if (response.getDstParent().getCapacity() < response.getDstFile().getDirOffset() + Constants.DIRECTORY_RECORD) {
			logger.info("rename: parent capacity does not match dst file offset, capacity " + response.getDstParent().getCapacity() + ", offset " + response.getDstFile().getDirOffset() + ", capacity " + dstParent.getCapacity() + ", offset " + dstFile.getDirOffset());
		}
		
		if (Constants.DEBUG) {
			logger.info("renameFile: src-parent " + srcParent.getFd() + ", src-file " + srcFile.getFd() + ", dst-parent " + dstParent.getFd() + ", dst-fd " + dstFile.getFd());
		}	
		
		return NamenodeErrors.ERR_OK;
	}	
	
	@Override
	public short getDataNode(NamenodeRequest.GetDataNode request, NamenodeResponse.GetDataNode response, NamenodeState errorState) throws Exception {
		//check protocol
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_GET_DATANODE, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		//get params
		DatanodeInfo dnInfo = request.getInfo();
		
		//rpc
		DatanodeBlocks dnInfoNn = blockStore.getDataNode(dnInfo);
		if (dnInfoNn == null) {
			return NamenodeErrors.ERR_DATANODE_NOT_REGISTERED;
		}
		
		dnInfoNn.touch();
		response.setServiceId(serviceId);
		response.setFreeBlockCount(dnInfoNn.getBlockCount());
		
		return NamenodeErrors.ERR_OK;
	}	

	@Override
	public short setBlock(NamenodeRequest.SetBlock request, NamenodeResponse.Void response, NamenodeState errorState) throws Exception {
		//check protocol
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_SET_BLOCK, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}		
		
		//get params
		BlockInfo region = new BlockInfo();
		region.setBlockInfo(request.getBlockInfo());
		
		short error = NamenodeErrors.ERR_OK;
		if (blockStore.regionExists(region)) {
			error = blockStore.updateRegion(region);
		} else {
			//rpc
			int realBlocks = (int) (((long) region.getLength()) / Constants.BLOCK_SIZE) ;
			long offset = 0;
			for (int i = 0; i < realBlocks; i++) {
				NameNodeBlockInfo nnBlock = new NameNodeBlockInfo(region, offset, (int) Constants.BLOCK_SIZE);
				error = blockStore.addBlock(nnBlock);
				offset += Constants.BLOCK_SIZE;
				
				if (error != NamenodeErrors.ERR_OK) {
					break;
				}
			}
		}
		
		return error;
	}

	@Override
	public short getBlock(NamenodeRequest.GetBlock request, NamenodeResponse.GetBlock response, NamenodeState errorState) throws Exception {
		//check protocol
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_GET_BLOCK, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		//get params
		long fd = request.getFd();
		long token = request.getToken();
		long position = request.getPosition();
		long capacity = request.getCapacity();
		
		//check params
		if (position < 0) {
			return NamenodeErrors.ERR_POSITION_NEGATIV;
		}
	
		//rpc
		AbstractNode fileInfo = fileTable.get(fd);
		if (fileInfo == null) {
			return NamenodeErrors.ERR_FILE_NOT_OPEN;
		}
		
		int index = Utils.computeIndex(position);
		if (index < 0) {
			return NamenodeErrors.ERR_POSITION_NEGATIV;
		}
		
		NameNodeBlockInfo block = fileInfo.getBlock(index);
		if (block == null && fileInfo.getToken() == token) {
			block = blockStore.getBlock(fileInfo.getStorageClass(), fileInfo.getLocationClass());
			if (block == null) {
				return NamenodeErrors.ERR_NO_FREE_BLOCKS;
			}
			if (!fileInfo.addBlock(index, block)) {
				return NamenodeErrors.ERR_ADD_BLOCK_FAILED;
			}
			block = fileInfo.getBlock(index);
			if (block == null) {
				return NamenodeErrors.ERR_ADD_BLOCK_FAILED;
			}
			fileInfo.setCapacity(capacity);
		} else if (block == null && token > 0) {
			return NamenodeErrors.ERR_TOKEN_MISMATCH;
		} else if (block == null && token == 0) {
			return NamenodeErrors.ERR_CAPACITY_EXCEEDED;
		} 
		
		response.setBlockInfo(block);
		return NamenodeErrors.ERR_OK;
	}
	
	@Override
	public short getLocation(NamenodeRequest.GetLocation request, NamenodeResponse.GetLocation response, NamenodeState errorState) throws Exception {
		//check protocol
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_GET_LOCATION, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		//get params
		FileName fileName = request.getFileName();
		long position = request.getPosition();
		
		//check params
		if (position < 0) {
			return NamenodeErrors.ERR_POSITION_NEGATIV;
		}	
		
		//rpc
		AbstractNode fileInfo = fileTree.retrieveFile(fileName, errorState);
		if (errorState.getError() != NamenodeErrors.ERR_OK) {
			return errorState.getError();
		}		
		if (fileInfo == null) {
			return NamenodeErrors.ERR_GET_FILE_FAILED;
		}	
		
		int index = Utils.computeIndex(position);
		if (index < 0) {
			return NamenodeErrors.ERR_POSITION_NEGATIV;
		}		
		BlockInfo block = fileInfo.getBlock(index);
		if (block == null) {
			return NamenodeErrors.ERR_OFFSET_TOO_LARGE;
		}
		
		response.setBlockInfo(block);
		
		return NamenodeErrors.ERR_OK;
	}

	//------------------------
	
	@Override
	public short dump(NamenodeRequest.DumpNameNode request, NamenodeResponse.Void response, NamenodeState errorState) throws Exception {
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_DUMP_NAMENODE, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}			
		
		System.out.println("#fd\t\tfilecomp\t\tcapacity\t\tisdir\t\t\tdiroffset");
		fileTree.dump();
		System.out.println("#fd\t\tfilecomp\t\tcapacity\t\tisdir\t\t\tdiroffset");
		dumpFastMap();
		
		return NamenodeErrors.ERR_OK;
	}	
	
	@Override
	public short ping(NamenodeRequest.PingNameNode request, NamenodeResponse.PingNameNode response, NamenodeState errorState) throws Exception {
		if (!NamenodeProtocol.verifyProtocol(NamenodeProtocol.CMD_PING_NAMENODE, request, response)) {
			return NamenodeErrors.ERR_PROTOCOL_MISMATCH;
		}	
		
		response.setData(request.getOp()+1);
		
		return NamenodeErrors.ERR_OK;
	}
	
	
	//--------------- helper functions
	
	void appendToDeleteQueue(AbstractNode fileInfo) {
		if (fileInfo != null) {
			fileInfo.setDelay(Constants.TOKEN_EXPIRATION);
			deleteQueue.add(fileInfo);			
		}
	}	
	
	void freeFile(AbstractNode fileInfo) throws Exception {
		if (fileInfo != null) {
			fileTable.remove(fileInfo.getFd());
			fileInfo.freeBlocks(blockStore);
		}
	}

	private void dumpFastMap() {
		for (Long key : fileTable.keySet()) {
			AbstractNode file = fileTable.get(key);
			System.out.println(file.toString());
		}		
	}
}
