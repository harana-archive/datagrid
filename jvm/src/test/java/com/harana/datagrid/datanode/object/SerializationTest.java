package com.harana.datagrid.datanode.object;

import com.harana.datagrid.datanode.object.rpc.MappingEntry;
import com.harana.datagrid.datanode.object.rpc.ObjectStoreRPC;
import com.harana.datagrid.datanode.object.rpc.RPCCall;
import io.netty.buffer.ByteBuf;
import org.apache.commons.collections.ListUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.netty.buffer.Unpooled.buffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class SerializationTest {
	private static final Logger logger = LogManager.getLogger();

	private static MappingEntry makeRandomObjectInfo() {
		Random rand = new Random();
		String key = String.valueOf(rand.nextDouble());
		key = "ABCDEFGHIJKLMN";
		long start = Math.abs(rand.nextLong());
		start = 12345689L;
		long end = Math.abs(rand.nextLong());
		end = 9876543210L;
		if (start > end) {
			long tmp = end;
			end = start;
			start = tmp;
		}
		return new MappingEntry(key, start, end);
	}

	private static short getMsgSize(ByteBuf buf) {
		return buf.getShort(0);
	}

	@BeforeClass
	public static void setUp() {
		logger.info(" ---------------------------------------------");
		logger.info(" *** Starting Serialization Tests ***");
		BasicConfigurator.configure();
	}

	@AfterClass
	public static void tearDown() {
		BasicConfigurator.resetConfiguration();
		logger.info(" *** End Serialization Tests ***");
		logger.info(" ---------------------------------------------\n\n");
	}

	@Test
	public void testObjectInfo() {
		Random rand = new Random();
		MappingEntry o1, o2;
		o1 = makeRandomObjectInfo();
		o2 = new MappingEntry(o1.getKey(), o1.getStartOffset(), o1.getSize());
		assertTrue(o1.equals(o2)); // check that MappingEntry comparison actually works
	}

	@Test
	public void testTranslateRpc() {
		Random rand = new Random();

		long cookie = rand.nextLong();
		long addr = Math.abs(rand.nextLong());
		int length = Math.abs(rand.nextInt());
		ObjectStoreRPC.TranslateBlock rpc1 =
				new ObjectStoreRPC.TranslateBlock(cookie, addr, length);
		ByteBuf buf = buffer(4096);
		rpc1.serializeRequest(buf);
		assertEquals(getMsgSize(buf), buf.readableBytes());
		ObjectStoreRPC.TranslateBlock rpc2 = new ObjectStoreRPC.TranslateBlock(buf);
		assertTrue(rpc1.getAddr() == rpc2.getAddr());
		assertTrue(rpc1.getCookie() == rpc2.getCookie());
		assertTrue(rpc1.getLength() == rpc2.getLength());
		assertTrue(rpc1.getRequestSize() == rpc2.getRequestSize());

		buf.clear();
		List<MappingEntry> mapping1 = new ArrayList<>();
		List<MappingEntry> mapping2;
		for (int i = 0; i < 10; i++) {
			mapping1.add(makeRandomObjectInfo());
		}
		rpc2.setResponse(mapping1);
		rpc2.serializeResponse(buf);
		assertEquals(getMsgSize(buf), buf.readableBytes());
		rpc1.deserializeResponse(buf);
		mapping2 = rpc2.getResponse();
		List commonMapping = ListUtils.subtract(mapping1, mapping2);
		assertTrue(commonMapping.size() == 0);
	}

	@Test
	public void testWriteBlockRpc() {
		Random rand = new Random();

		long cookie = rand.nextLong();
		long addr = Math.abs(rand.nextLong());
		int length = Math.abs(rand.nextInt());
		int lkey = rand.nextInt();
		String key = "ABCDEFGHIJKLMN";
		ObjectStoreRPC.WriteBlock rpc1 =
				new ObjectStoreRPC.WriteBlock(cookie, addr, length, key);
		ByteBuf buf = buffer(4096);
		rpc1.serializeRequest(buf);
		assertEquals(getMsgSize(buf), buf.readableBytes());
		ObjectStoreRPC.WriteBlock rpc2 = new ObjectStoreRPC.WriteBlock(buf);
		assertTrue(rpc1.getAddr() == rpc2.getAddr());
		assertTrue(rpc1.getCookie() == rpc2.getCookie());
		assertTrue(rpc1.getLength() == rpc2.getLength());
		assertTrue(rpc1.getRequestSize() == rpc2.getRequestSize());
		assertTrue(rpc1.getObjectKey().equals(rpc2.getObjectKey()));

		buf.clear();
		rpc2.setResponseStatus(RPCCall.SUCCESS);
		rpc2.serializeResponse(buf);
		assertEquals(getMsgSize(buf), buf.readableBytes());
		rpc1.deserializeResponse(buf);
		assertTrue(rpc1.getStatus() == rpc2.getStatus());
	}

	@Test
	public void testWriteBlockRangeRpc() {
		Random rand = new Random();

		long cookie = rand.nextLong();
		long addr = Math.abs(rand.nextLong());
		int length = Math.abs(rand.nextInt());
		String key = "ABCDEFGHIJKLMN";
		ObjectStoreRPC.WriteBlockRange rpc1 =
				new ObjectStoreRPC.WriteBlockRange(cookie, addr, 100L, length, key);
		ByteBuf buf = buffer(4096);
		rpc1.serializeRequest(buf);
		assertEquals(getMsgSize(buf), buf.readableBytes());
		ObjectStoreRPC.WriteBlockRange rpc2 = new ObjectStoreRPC.WriteBlockRange(buf);
		assertTrue(rpc1.getAddr() == rpc2.getAddr());
		assertTrue(rpc1.getOffset() == rpc2.getOffset());
		assertTrue(rpc1.getCookie() == rpc2.getCookie());
		assertTrue(rpc1.getLength() == rpc2.getLength());
		assertTrue(rpc1.getRequestSize() == rpc2.getRequestSize());
		assertTrue(rpc1.getObjectKey().equals(rpc2.getObjectKey()));

		buf.clear();
		rpc2.setResponseStatus(RPCCall.SUCCESS);
		rpc2.serializeResponse(buf);
		assertEquals(getMsgSize(buf), buf.readableBytes());
		rpc1.deserializeResponse(buf);
		assertTrue(rpc1.getStatus() == rpc2.getStatus());
	}

	@Test
	public void testUnmapBlockRpc() {
		Random rand = new Random();

		long cookie = rand.nextLong();
		long addr = Math.abs(rand.nextLong());
		int length = Math.abs(rand.nextInt());
		ObjectStoreRPC.UnmapBlock rpc1 = new ObjectStoreRPC.UnmapBlock(cookie, addr, length);
		ByteBuf buf = buffer(4096);
		rpc1.serializeRequest(buf);
		assertEquals(getMsgSize(buf), buf.readableBytes());
		ObjectStoreRPC.UnmapBlock rpc2 = new ObjectStoreRPC.UnmapBlock(buf);
		assertTrue(rpc1.getAddr() == rpc2.getAddr());
		assertTrue(rpc1.getCookie() == rpc2.getCookie());
		assertTrue(rpc1.getLength() == rpc2.getLength());
		assertTrue(rpc1.getRequestSize() == rpc2.getRequestSize());

		buf.clear();
		rpc2.setResponseStatus(RPCCall.SUCCESS);
		rpc2.serializeResponse(buf);
		assertEquals(getMsgSize(buf), buf.readableBytes());
		rpc1.deserializeResponse(buf);
		assertTrue(rpc1.getStatus() == rpc2.getStatus());
	}
}
