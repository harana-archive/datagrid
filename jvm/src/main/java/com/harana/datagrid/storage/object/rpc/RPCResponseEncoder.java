package com.harana.datagrid.storage.object.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RPCResponseEncoder extends MessageToByteEncoder<RPCCall> {

	private static Logger logger = LogManager.getLogger();

	@Override
	protected void encode(ChannelHandlerContext channelHandlerContext, RPCCall resp, ByteBuf byteBuf) {
		resp.serializeResponse(byteBuf);
		//logger.debug("Response encoded (RpcID = {}, size = {})  ", resp.getCmd(), byteBuf.readableBytes());
	}
}