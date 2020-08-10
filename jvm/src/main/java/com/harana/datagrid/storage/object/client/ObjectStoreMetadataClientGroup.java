package com.harana.datagrid.storage.object.client;

import com.harana.datagrid.storage.object.ObjectStoreConstants;
import com.harana.datagrid.storage.object.ObjectStoreUtils;
import com.harana.datagrid.storage.object.rpc.ObjectStoreResponseDecoder;
import com.harana.datagrid.storage.object.rpc.RPCCall;
import com.harana.datagrid.storage.object.rpc.RPCFuture;
import com.harana.datagrid.storage.object.rpc.RPCRequestEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ObjectStoreMetadataClientGroup {
	private static final Logger logger = LogManager.getLogger();
	private final EventLoopGroup workerGroup;
	private final Bootstrap boot;
	private final InetSocketAddress metadataServerAddress;

	private final ConcurrentHashMap<Long, RPCFuture<RPCCall>> inFlightOps;
	private final AtomicLong slot;

	private final ArrayList<ObjectStoreMetadataClient> activeClients;

	public ObjectStoreMetadataClientGroup() {
		workerGroup = new NioEventLoopGroup(4);
		boot = new Bootstrap();
		boot.group(workerGroup);
		boot.channel(NioSocketChannel.class);
		boot.option(ChannelOption.SO_KEEPALIVE, true);
		boot.option(ChannelOption.TCP_NODELAY, true);
		boot.option(ChannelOption.SO_REUSEADDR, true);
		boot.option(ChannelOption.SO_RCVBUF, 1048576);
		boot.option(ChannelOption.SO_SNDBUF, 1048576);
		final ObjectStoreMetadataClientGroup thisGroup = this;
		metadataServerAddress =
				new InetSocketAddress(ObjectStoreConstants.DATANODE, ObjectStoreConstants.DATANODE_PORT);
		boot.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) {
				/* outgoing pipeline */
				ch.pipeline().addLast(new RPCRequestEncoder());
				/* incoming pipeline */
				ch.pipeline().addLast(new ObjectStoreResponseDecoder(thisGroup));
			}
		});

		slot = new AtomicLong(1);
		inFlightOps = new ConcurrentHashMap<>();
		activeClients = new ArrayList<>();
	}

	public void closeClientGroup() {
		logger.info("Closing ObjectStore Client Group");
		/* check in flight */
		if (inFlightOps.size() != 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		for (ObjectStoreMetadataClient c : activeClients) {
			c.close();
		}
		activeClients.clear();
		try {
			workerGroup.shutdownGracefully().sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (inFlightOps.size() != 0) {
			logger.error("There are in flight requests");
			Thread.dumpStack();
		}
	}

	public long getNextSlot() {
		return slot.incrementAndGet();
	}

	public void insertNewInflight(long slot, RPCFuture<RPCCall> rpc) {
		logger.debug("Inserted RPC with cookie = {}", slot);
		inFlightOps.put(slot, rpc);
	}

	public RPCFuture<RPCCall> retrieveAndRemove(long slot) {
		return inFlightOps.remove(slot);
	}

	public ObjectStoreMetadataClient getClient() {
		Channel clientChannel = null;
		try {
			clientChannel = boot.connect(metadataServerAddress).sync().channel();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.info("Connected to the ObjectStore Metadata Server at {}", metadataServerAddress);
		ObjectStoreMetadataClient ep = new ObjectStoreMetadataClient(clientChannel, this);
		activeClients.add(ep);
		return ep;
	}

	public void dumpInFligh() {
		logger.info("In Flight RPCs registered:");
		for (long cookie : inFlightOps.keySet()) {
			logger.info("   - Rpc with cookie = {}", cookie);
		}
	}

	public int getInFlight() {
		return inFlightOps.size();
	}
}
