package com.harana.datagrid.datanode.rdma.client;

import com.harana.datagrid.DatagridBuffer;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.client.datanode.DatanodeFuture;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.datanode.DatanodeUtils;
import com.harana.datagrid.datanode.rdma.RdmaConstants;
import com.harana.datagrid.memory.OffHeapBuffer;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.rdma.verbs.IbvQP;
import com.harana.datagrid.rdma.verbs.RdmaCmId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class RdmaStorageLocalEndpoint implements DatanodeEndpoint {
	private static final Logger logger = LogManager.getLogger();
	private final ConcurrentHashMap<Long, DatagridBuffer> bufferMap;
	private final Unsafe unsafe;
	private final InetSocketAddress address;
	
	public RdmaStorageLocalEndpoint(InetSocketAddress datanodeAddr) throws Exception {
		logger.info("new local endpoint for address " + datanodeAddr);
		String dataPath = DatanodeUtils.getDatanodeDirectory(RdmaConstants.STORAGE_RDMA_DATA_PATH, datanodeAddr);
		File dataDir = new File(dataPath);
		if (!dataDir.exists()) {
			throw new Exception("Local RDMA data path missing");
		}
		this.address = datanodeAddr;
		this.bufferMap = new ConcurrentHashMap<>();
		this.unsafe = getUnsafe();
		for (File dataFile : Objects.requireNonNull(dataDir.listFiles())) {
			long lba = Long.parseLong(dataFile.getName());
			OffHeapBuffer offHeapBuffer = OffHeapBuffer.wrap(mmap(dataFile));
			bufferMap.put(lba, offHeapBuffer);
		}				
	}

	@Override
	public DatanodeFuture write(DatagridBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException {
		if (buffer.remaining() > DatagridConstants.BLOCK_SIZE) {
			throw new IOException("write size too large " + buffer.remaining());
		}
		if (buffer.remaining() <= 0) {
			throw new IOException("write size too small, len " + buffer.remaining());
		}	
		if (remoteOffset < 0) {
			throw new IOException("remote offset too small " + remoteOffset);
		}	
		
		long alignedLba = getAlignedLba(remoteMr.getLba());
		long lbaOffset = getLbaOffset(remoteMr.getLba());
		
		DatagridBuffer mappedBuffer = bufferMap.get(alignedLba);
		if (mappedBuffer == null) {
			throw new IOException("No mapped buffer for this key " + remoteMr.getLkey() + ", address " + address);
		}
		
		if (lbaOffset + remoteOffset + buffer.remaining() > RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE) {
			long tmpAddr = lbaOffset + remoteOffset + buffer.remaining();
			throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
		}		
		long srcAddr = buffer.address() + buffer.position();
		long dstAddr = mappedBuffer.address() + lbaOffset + remoteOffset;
		return new RdmaLocalFuture(unsafe, srcAddr, dstAddr, buffer.remaining());
	}

	@Override
	public DatanodeFuture read(DatagridBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException {
		if (buffer.remaining() > DatagridConstants.BLOCK_SIZE) {
			throw new IOException("read size too large");
		}	
		if (buffer.remaining() <= 0) {
			throw new IOException("read size too small, len " + buffer.remaining());
		}
		if (remoteOffset < 0) {
			throw new IOException("remote offset too small " + remoteOffset);
		}
		if (buffer.position() < 0) {
			throw new IOException("local offset too small " + buffer.position());
		}
		
		long alignedLba = getAlignedLba(remoteMr.getLba());
		long lbaOffset = getLbaOffset(remoteMr.getLba());
		
		DatagridBuffer mappedBuffer = bufferMap.get(alignedLba);
		if (mappedBuffer == null) {
			throw new IOException("No mapped buffer for this key");
		}
		if (lbaOffset + remoteOffset + buffer.remaining() > RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE) {
			long tmpAddr = lbaOffset + remoteOffset + buffer.remaining();
			throw new IOException("remote fileOffset + remoteOffset + len too large " + tmpAddr);
		}			
		long srcAddr = mappedBuffer.address() + lbaOffset + remoteOffset;
		long dstAddr = buffer.address() + buffer.position();
		return new RdmaLocalFuture(unsafe, srcAddr, dstAddr, buffer.remaining());
	}
	
	private static long getAlignedLba(long remoteLba) {
		return remoteLba / RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE;
	}
	
	private static long getLbaOffset(long remoteLba) {
		return remoteLba % RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE;
	}

	@Override
	public void close() throws InterruptedException {
	}

	public void connect(SocketAddress inetAddress, int i) {
	}

	public int getEndpointId() {
		return 0;
	}

	public int getFreeSlots() {
		return 0;
	}

	public boolean isConnected() {
		return true;
	}

	public IbvQP getQp() {
		return null;
	}

	public String getAddress() {
		return null;
	}

	public RdmaCmId getContext() {
		return null;
	}
	
	private MappedByteBuffer mmap(File file) throws IOException{
		RandomAccessFile randomFile = new RandomAccessFile(file.getAbsolutePath(), "rw");
		FileChannel channel = randomFile.getChannel();
		MappedByteBuffer mappedBuffer = channel.map(MapMode.READ_WRITE, 0, RdmaConstants.STORAGE_RDMA_ALLOCATION_SIZE);
		randomFile.close();
		return mappedBuffer;
	}	
	
	private Unsafe getUnsafe() throws Exception {
		Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
		theUnsafe.setAccessible(true);
		return (Unsafe) theUnsafe.get(null);
	}

	@Override
	public boolean isLocal() {
		return true;
	}
}