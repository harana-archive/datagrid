package com.harana.datagrid.datanode.object;

import com.harana.datagrid.datanode.object.client.ObjectStoreMetadataClient;
import com.harana.datagrid.datanode.object.client.ObjectStoreMetadataClientGroup;
import com.harana.datagrid.datanode.object.rpc.ObjectStoreRPC;
import com.harana.datagrid.datanode.object.rpc.RPCCall;
import com.harana.datagrid.datanode.object.server.ObjectStoreMetadataServer;
import com.harana.datagrid.conf.Constants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeInfo;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class NettyPerformanceTest {
	private static final Logger logger = LogManager.getLogger();
	private final long addr = 0;
	private final int threadNr = 8;
	private ObjectStoreMetadataClientGroup clientGroup;
	private ObjectStoreMetadataClient client;
	private ObjectStoreMetadataServer server;

	@BeforeClass
	public static void setUp() {
		BasicConfigurator.configure();
		logger.info(" ---------------------------------------------");
		logger.info(" --- Starting NettyPerformanceTests ---");
		ObjectStoreConstants.PROFILE = false;
		ObjectStoreConstants.DATANODE = "127.0.0.1";
	}

	@AfterClass
	public static void tearDown() {
		logger.info(" --- End NettyPerformanceTests       ---");
		logger.info(" ---------------------------------------------\n\n");
		BasicConfigurator.resetConfiguration();
		ObjectStoreConstants.PROFILE = false;
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Before
	public void setUpTest() {
		logger.info(" *** Setting up test ***");
		server = new ObjectStoreMetadataServer(); // create server first
		server.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		clientGroup = new ObjectStoreMetadataClientGroup();
		client = clientGroup.getClient();
	}

	@After
	public void tearDownTest() {
		logger.info(" *** Tearing down test ***\n");
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
	public void testBasicPerformance() throws Exception {
		logger.info(" *** Netty single thread latency test ***");
		OneRPCRun run = new OneRPCRun(this.client, 0);
		Thread thread = new Thread(run);
		thread.start();
		thread.join();
		assertEquals(run.getStatus(), 0);
	}

	@Test
	public void testRPCConcurrency() throws Exception {
		logger.info(" *** Netty concurrency test ***");
		Thread[] threads = new Thread[threadNr];
		OneRPCRun[] runs = new OneRPCRun[threadNr];
		for (int i = 0; i < threadNr; i++) {
			runs[i] = new OneRPCRun(this.client, i);
			threads[i] = new Thread(runs[i]);
			threads[i].start();
		}
		for (int i = 0; i < threadNr; i++) {
			threads[i].join();
			assertEquals(runs[i].getStatus(), 0);
		}
	}

	private class OneRPCRun implements Runnable {
		private ObjectStoreMetadataClient client;
		private int id;
		private volatile int exitStatus = -1;

		public OneRPCRun(ObjectStoreMetadataClient client, int id) {
			this.client = client;
			this.id = id;
		}

		public void run() {
			Future<RPCCall> rpcFuture;
			ObjectStoreRPC.TranslateBlock translateBlock;
			long startTime, endTime, runtime;
			final int iterations = 100000;

			startTime = System.nanoTime();
			try {
				// write block and retrive block to object mappping
				for (int i = 0; i < iterations; i++) {
					BlockInfo bi = new BlockInfo(new DatanodeInfo(),
							addr + id * ObjectStoreConstants.ALLOCATION_SIZE,
							addr + id * ObjectStoreConstants.ALLOCATION_SIZE,
							(int) Constants.BLOCK_SIZE,
							123456);
					rpcFuture = client.writeBlock(bi, "objkey" + id);
					ObjectStoreRPC.WriteBlock writeBlock = (ObjectStoreRPC.WriteBlock) rpcFuture.get();
					assertEquals(writeBlock.getStatus(), RPCCall.SUCCESS);
				}
				endTime = System.nanoTime();
				runtime = endTime - startTime;
				logger.info("{} WriteBlock RPCs in {} seconds. Average latency {}  (us)", iterations,
						runtime / 1000000000.,
						runtime / iterations / 1000.);


				startTime = System.nanoTime();
				// write block and retrive block to object mappping
				for (int i = 0; i < iterations; i++) {
					BlockInfo bi = new BlockInfo(new DatanodeInfo(),
							addr + id * ObjectStoreConstants.ALLOCATION_SIZE,
							addr + id * ObjectStoreConstants.ALLOCATION_SIZE,
							(int) Constants.BLOCK_SIZE,
							123456);
					rpcFuture = client.translateBlock(bi);
					translateBlock = (ObjectStoreRPC.TranslateBlock) rpcFuture.get();
					assertEquals(translateBlock.getStatus(), RPCCall.SUCCESS);
				}
				endTime = System.nanoTime();
				runtime = endTime - startTime;
				logger.info("{} TranslateBlock RPCs in {} seconds. Average latency {} (us)", iterations,
						runtime / 1000000000.,
						runtime / iterations / 1000.);
				exitStatus = 0;
			} catch (Exception e) {
				logger.error("Got exception " + e);
				return;
			}
		}

		public int getStatus() {
			return exitStatus;
		}
	}
}
