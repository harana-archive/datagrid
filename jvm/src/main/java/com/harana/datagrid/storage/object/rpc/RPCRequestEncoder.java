package com.harana.datagrid.storage.object.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RPCRequestEncoder extends MessageToByteEncoder<RPCCall> {
	private static Logger logger = LogManager.getLogger();

	@Override
	protected void encode(ChannelHandlerContext channelHandlerContext, RPCCall req, ByteBuf byteBuf) {
		int bytes = req.serializeRequest(byteBuf);
		int bytes2 = byteBuf.readableBytes();
		assert bytes == bytes2;
		//logger.debug("Request encoded (RpcID = {}, size = {})  ", req.getCmd(), byteBuf.readableBytes());
	}
}
