package com.harana.datagrid.storage.object.rpc;

import com.harana.datagrid.storage.object.ObjectStoreUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ObjectStoreRequestDecoder extends ByteToMessageDecoder {
	private static Logger logger = LogManager.getLogger();

	@Override
	protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
		/* Wait for a complete message. The first 2 bytes represent the message length */
		//logger.debug("Decoding request ({} bytes available)" , byteBuf.readableBytes());
		if (!RPCCall.isMessageComplete(byteBuf)) {
			return;
		}
		/* Full message, we can now decode */
		RPCCall newReq = ObjectStoreRPC.createObjectStoreRPC(byteBuf);
		//logger.debug("After decoding, {} bytes still remaining", byteBuf.readableBytes());
		list.add(newReq);
	}
}
