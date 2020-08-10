package com.harana.datagrid.datanode.object.rpc;

import com.harana.datagrid.datanode.object.client.ObjectStoreMetadataClientGroup;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ObjectStoreResponseDecoder extends ByteToMessageDecoder {

	private static Logger logger = LogManager.getLogger();
	private final ObjectStoreMetadataClientGroup group;

	public ObjectStoreResponseDecoder(ObjectStoreMetadataClientGroup group) {
		this.group = group;
	}

	@Override
	protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) {
		logger.debug("Decoding response ({} bytes available)", byteBuf.readableBytes());
		while (RPCCall.isMessageComplete(byteBuf)) {
			long cookie = RPCCall.getCookie(byteBuf);
			RPCFuture<RPCCall> future = group.retrieveAndRemove(cookie);
			if (future != null) {
				RPCCall rpc = future.getResult();
				//logger.debug("Received response for Cookie = {}, RpcID = {}", rpc.getCookie(), rpc.getCmd());
				rpc.deserializeResponse(byteBuf);
				future.markDone();
			} else {
				// This can happen in several scenarios: (a) network issues, (b) client kill and restart
				logger.warn("Received response to non registered RPC. Buffer reader index = {}, readable bytes = {}, " + "Cookie = {}. Draining message...", byteBuf.readerIndex(), byteBuf.readableBytes(), cookie);
				if (group.getInFlight() == 0) {
					byteBuf.clear();
				} else {
					group.dumpInFligh();
					RPCCall.drainMessage(byteBuf);
				}
			}
		}
	}
}
