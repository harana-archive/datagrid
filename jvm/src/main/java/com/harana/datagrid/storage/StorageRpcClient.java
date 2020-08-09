package com.harana.datagrid.storage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.harana.datagrid.CrailLocationClass;
import com.harana.datagrid.CrailStorageClass;
import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.metadata.DataNodeStatistics;
import com.harana.datagrid.rpc.RpcConnection;
import com.harana.datagrid.rpc.RpcErrors;
import com.harana.datagrid.rpc.RpcVoid;
import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

public class StorageRpcClient {
	public static final Logger logger = LogManager.getLogger();
	
	private InetSocketAddress serverAddress;
	private int storageType;
	private CrailStorageClass storageClass;
	private CrailLocationClass locationClass;
	private RpcConnection rpcConnection;	

	public StorageRpcClient(int storageType, CrailStorageClass storageClass, InetSocketAddress serverAddress, RpcConnection rpcConnection) throws Exception {
		this.storageType = storageType;
		this.storageClass = storageClass;
		this.serverAddress = serverAddress;
		this.locationClass = CrailUtils.getLocationClass();
		this.rpcConnection = rpcConnection;
	}
	
	public void setBlock(long lba, long addr, int length, int key) throws Exception {
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageType, storageClass.value(), locationClass.value(), inetAddress.getAddress().getAddress(), inetAddress.getPort());
		BlockInfo blockInfo = new BlockInfo(dnInfo, lba, addr, length, key);
		RpcVoid res = rpcConnection.setBlock(blockInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS);
		if (res.getError() != RpcErrors.ERR_OK){
			logger.info("setBlock: " + RpcErrors.messages[res.getError()]);
			throw new IOException("setBlock: " + RpcErrors.messages[res.getError()]);
		}
	}
	
	public DataNodeStatistics getDataNode() throws Exception{
		InetSocketAddress inetAddress = serverAddress;
		DataNodeInfo dnInfo = new DataNodeInfo(storageType, storageClass.value(), locationClass.value(), inetAddress.getAddress().getAddress(), inetAddress.getPort());
		return this.rpcConnection.getDataNode(dnInfo).get(CrailConstants.RPC_TIMEOUT, TimeUnit.MILLISECONDS).getStatistics();
	}	
}
