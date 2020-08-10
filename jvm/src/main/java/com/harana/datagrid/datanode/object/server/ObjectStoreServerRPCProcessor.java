package com.harana.datagrid.datanode.object.server;

import com.harana.datagrid.datanode.object.rpc.ObjectStoreRPC;
import com.harana.datagrid.datanode.object.rpc.RPCCall;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ObjectStoreServerRPCProcessor extends SimpleChannelInboundHandler<RPCCall> {
	static private final Logger logger = LogManager.getLogger();
	static private ObjectStoreMetadataServer service;

	static public void setMetadataService(ObjectStoreMetadataServer service) {
		ObjectStoreServerRPCProcessor.service = service;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, RPCCall rpc) throws Exception {
		try {
			switch (rpc.getCmd()) {
				case ObjectStoreRPC.TranslateBlockCmd:
					service.translateBlock((ObjectStoreRPC.TranslateBlock) rpc);
					break;
				case ObjectStoreRPC.WriteBlockCmd:
					service.writeBlock((ObjectStoreRPC.WriteBlock) rpc);
					break;
				case ObjectStoreRPC.WriteBlockRangeCmd:
					service.writeBlockRange((ObjectStoreRPC.WriteBlockRange) rpc);
					break;
				case ObjectStoreRPC.UnmapBlockCmd:
					service.unmapBlock((ObjectStoreRPC.UnmapBlock) rpc);
					break;
				default:
					logger.error("Ignoring invalid RPC command not valid (RpcID = {})", rpc.getCmd());
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}

		try {
			/* flush response */
			ctx.channel().writeAndFlush(rpc);
		} catch (Exception e) {
			logger.error("Failed writing RPC response (RpcID = {})", rpc.getCmd());
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	protected void finalize() {
		logger.info("Closing ObjectStore RPC processor");
	}
}
