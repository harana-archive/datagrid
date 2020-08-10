package com.harana.datagrid.storage.object;

import com.harana.datagrid.storage.object.client.ObjectStoreMetadataClient;
import com.harana.datagrid.storage.object.client.ObjectStoreMetadataClientGroup;
import com.harana.datagrid.storage.object.rpc.MappingEntry;
import com.harana.datagrid.storage.object.rpc.ObjectStoreRPC;
import com.harana.datagrid.storage.object.rpc.RPCCall;
import com.harana.datagrid.storage.object.server.ObjectStoreMetadataServer;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DataNodeInfo;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class BasicRPCTest {
	private static final Logger logger = LogManager.getLogger();
	private final long addr = 1234;
	private final int length = 1000000;
	private ObjectStoreMetadataClientGroup clientGroup;
	private ObjectStoreMetadataClient client;
	private ObjectStoreMetadataServer server;

	@BeforeClass
	public static void setUp() {
		BasicConfigurator.configure();
		logger.info(" ---------------------------------------------");
		logger.info(" --- Starting Basic RPC Tests ---");
		ObjectStoreConstants.PROFILE = true;
		ObjectStoreConstants.DATANODE = "127.0.0.1";
	}

	@AfterClass
	public static void tearDown() {
		BasicConfigurator.resetConfiguration();
		ObjectStoreConstants.PROFILE = false;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		logger.info(" --- End Basic RPC Tests ---");
		logger.info(" ---------------------------------------------\n\n");
	}

	@Before
	public void setUpTest() {
		logger.info(" *** Set up test ***");
		server = new ObjectStoreMetadataServer();
		server.start();
		clientGroup = new ObjectStoreMetadataClientGroup();
		client = clientGroup.getClient();
	}

	@After
	public void tearDownTest() {
		logger.info(" *** Tear down test ***");
		client.close();
		clientGroup.closeClientGroup();
		server.close();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testBasicRpc() throws Exception {
		logger.info(" *** testBasicRpc() ***");
		BlockInfo bi = new BlockInfo(new DataNodeInfo(), addr, addr, length, 123456);
		Future<RPCCall> rpcFuture;
		ObjectStoreRPC.TranslateBlock translateBlock;

		// read non-existing block
		rpcFuture = client.translateBlock(bi);
		translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
		assertEquals(translateBlock.getStatus(), RPCCall.NO_MATCH);
		rpcFuture = client.translateBlock(bi);
		translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
		assertEquals(translateBlock.getStatus(), RPCCall.NO_MATCH);

		// write block and retrive block to object mappping
		rpcFuture = client.writeBlock(bi, "key");
		ObjectStoreRPC.WriteBlock writeBlock = (ObjectStoreRPC.WriteBlock) rpcFuture.get();
		assertEquals(writeBlock.getStatus(), RPCCall.SUCCESS);
		rpcFuture = client.translateBlock(bi);
		translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
		assertEquals(translateBlock.getStatus(), RPCCall.SUCCESS);
		List<MappingEntry> blockMapping = translateBlock.getResponse();
		assertEquals(blockMapping.size(), 1);
		assertEquals(blockMapping.get(0).getKey(), "key");

		// delete block & verify
		rpcFuture = client.unmapBlock(bi);
		ObjectStoreRPC.UnmapBlock unmapBlock = (ObjectStoreRPC.UnmapBlock) rpcFuture.get();
		assertEquals(unmapBlock.getStatus(), RPCCall.SUCCESS);
		rpcFuture = client.translateBlock(bi);
		translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
		assertEquals(translateBlock.getStatus(), RPCCall.NO_MATCH);
	}
}
