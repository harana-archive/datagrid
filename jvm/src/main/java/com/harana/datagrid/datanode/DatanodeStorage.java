package com.harana.datagrid.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.harana.datagrid.LocationClass;
import com.harana.datagrid.StorageClass;
import com.harana.datagrid.client.namenode.NamenodeConnection;
import com.harana.datagrid.client.namenode.NamenodeErrors;
import com.harana.datagrid.client.namenode.NamenodeVoid;
import com.harana.datagrid.conf.Constants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.metadata.DatanodeStatistics;
import com.harana.datagrid.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatanodeStorage {
	public static final Logger logger = LogManager.getLogger();
	
	private final InetSocketAddress serverAddress;
	private final int storageType;
	private final StorageClass storageClass;
	private final LocationClass locationClass;
	private final NamenodeConnection namenodeConnection;

	public DatanodeStorage(int storageType, StorageClass storageClass, InetSocketAddress serverAddress, NamenodeConnection namenodeConnection) throws Exception {
		this.storageType = storageType;
		this.storageClass = storageClass;
		this.serverAddress = serverAddress;
		this.locationClass = Utils.getLocationClass();
		this.namenodeConnection = namenodeConnection;
	}
	
	public void setBlock(long lba, long addr, int length, int key) throws Exception {
		InetSocketAddress inetAddress = serverAddress;
		DatanodeInfo dnInfo = new DatanodeInfo(storageType, storageClass.value(), locationClass.value(), inetAddress.getAddress().getAddress(), inetAddress.getPort());
		BlockInfo blockInfo = new BlockInfo(dnInfo, lba, addr, length, key);
		NamenodeVoid res = this.namenodeConnection.setBlock(blockInfo).get(Constants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (res.getError() != NamenodeErrors.ERR_OK) {
			logger.info("setBlock: " + NamenodeErrors.messages[res.getError()]);
			throw new IOException("setBlock: " + NamenodeErrors.messages[res.getError()]);
		}
	}
	
	public DatanodeStatistics getDataNode() throws Exception {
		InetSocketAddress inetAddress = serverAddress;
		DatanodeInfo dnInfo = new DatanodeInfo(storageType, storageClass.value(), locationClass.value(), inetAddress.getAddress().getAddress(), inetAddress.getPort());
		return this.namenodeConnection.getDataNode(dnInfo).get(Constants.RPC_TIMEOUT, TimeUnit.MILLISECONDS).getStatistics();
	}	
}