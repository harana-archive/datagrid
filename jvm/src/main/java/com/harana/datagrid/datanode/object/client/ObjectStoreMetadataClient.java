package com.harana.datagrid.datanode.object.client;

import com.harana.datagrid.datanode.object.rpc.ObjectStoreRPC;
import com.harana.datagrid.datanode.object.rpc.RPCCall;
import com.harana.datagrid.datanode.object.rpc.RPCFuture;
import io.netty.channel.Channel;
import com.harana.datagrid.metadata.BlockInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Future;

public class ObjectStoreMetadataClient {

	static private final Logger logger = LogManager.getLogger();
	private final Channel clientChannel;
	private final ObjectStoreMetadataClientGroup group;

	public ObjectStoreMetadataClient(Channel clientChannel, ObjectStoreMetadataClientGroup grp) {
		logger.debug("Creating new ObjectStore metadata client");
		this.clientChannel = clientChannel;
		this.group = grp;
	}

	public String toString() {
		return this.clientChannel.toString();
	}

	public void close() {
		/* don't care about the future */
		logger.info("Closing ObjectStore Metadata Client");
		this.clientChannel.close();
	}

	public Future<RPCCall> translateBlock(BlockInfo blockInfo) {
		long cookie = this.group.getNextSlot();
		RPCCall rpc = new ObjectStoreRPC.TranslateBlock(cookie, blockInfo.getAddr(), blockInfo.getLength());
		return sendRPC(rpc);
	}

	private Future<RPCCall> sendRPC(RPCCall rpc) {
		//logger.debug("Sending new RPC. Cookie = {}, RpcID = {}", rpc.getCookie(), rpc.getCmd());
		RPCFuture<RPCCall> resultFuture = new RPCFuture<RPCCall>("RpcID " + rpc.getCmd(), rpc);
		/* rpc goes into the map */
		this.group.insertNewInflight(rpc.getCookie(), resultFuture);
		/* now we construct and push out the request */
		this.clientChannel.writeAndFlush(rpc);
		return resultFuture;
	}

	public Future<RPCCall> writeBlock(BlockInfo blockInfo, String newKey) {
		long cookie = this.group.getNextSlot();
		RPCCall rpc = new ObjectStoreRPC.WriteBlock(cookie, blockInfo.getAddr(), blockInfo.getLength(), newKey);
		return sendRPC(rpc);
	}

	public Future<RPCCall> unmapBlock(BlockInfo blockInfo) {
		long cookie = this.group.getNextSlot();
		RPCCall rpc = new ObjectStoreRPC.UnmapBlock(cookie, blockInfo.getAddr(), blockInfo.getLength());
		return sendRPC(rpc);
	}

	public Future<RPCCall> writeBlockRange(BlockInfo blockInfo, long blockOffset, int length, String newKey) {
		long cookie = this.group.getNextSlot();
		RPCCall rpc = new ObjectStoreRPC.WriteBlockRange(cookie, blockInfo.getAddr(), blockOffset, length, newKey);
		return sendRPC(rpc);
	}
}
