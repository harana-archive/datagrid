package com.harana.datagrid.datanode.rdma.client;

import com.harana.datagrid.DatagridBuffer;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.client.datanode.DatanodeFuture;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.datanode.rdma.MrCache;
import com.harana.datagrid.datanode.rdma.MrCache.DeviceMrCache;
import com.harana.datagrid.datanode.rdma.RdmaConstants;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.rdma.RdmaEndpoint;
import com.harana.datagrid.rdma.verbs.*;
import com.harana.datagrid.rdma.verbs.SVCPostSend.SendWRMod;
import com.harana.datagrid.rdma.verbs.SVCPostSend.SgeMod;
import com.harana.datagrid.utils.AtomicIntegerModulo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class RdmaStoragePassiveEndpoint extends RdmaEndpoint implements DatanodeEndpoint {
	private static final Logger logger = LogManager.getLogger();
	
	private final LinkedBlockingQueue<SVCPostSend> writeOps;
	private final LinkedBlockingQueue<SVCPostSend> readOps;
	private final AtomicIntegerModulo opcount;
	private final ReentrantLock lock;
	private final Semaphore sendQueueAvailable;
	private final ConcurrentHashMap<Long, RdmaPassiveFuture> futureMap;
	private final MrCache mrCache;

	private DeviceMrCache deviceCache;
	private IbvWC[] wcList;
	private SVCPollCq poll;

	public RdmaStoragePassiveEndpoint(RdmaStoragePassiveGroup group, RdmaCmId id, boolean serverSide) throws IOException {
		super(group, id, serverSide);
		writeOps = new LinkedBlockingQueue<>();
		readOps = new LinkedBlockingQueue<>();
		this.opcount = new AtomicIntegerModulo();
		this.lock = new ReentrantLock();
		this.sendQueueAvailable = new Semaphore(RdmaConstants.STORAGE_RDMA_QUEUESIZE);
		this.futureMap = new ConcurrentHashMap<>();
		this.mrCache = group.getMrCache();
		this.deviceCache = null;
	}

	@Override
	protected synchronized void init() throws IOException {
		super.init();
		
		for (int i = 0; i < RdmaConstants.STORAGE_RDMA_QUEUESIZE; i++) {
			SVCPostSend write = initWriteOp();
			writeOps.add(write);
			SVCPostSend read = initReadOp();
			readOps.add(read);
		}
		
		IbvCQ cq = getCqProvider().getCQ();
		this.wcList = new IbvWC[getCqProvider().getCqSize()];
		for (int i = 0; i < wcList.length; i++) {
			wcList[i] = new IbvWC();
		}		
		this.poll = cq.poll(wcList, wcList.length);			
	}
	
	private SVCPostSend initWriteOp() throws IOException {
		LinkedList<IbvSendWR> wrList_send = new LinkedList<>();
		
		IbvSendWR writeWR = new IbvSendWR();
		writeWR.setWr_id(opcount.getAndIncrement());
		writeWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE);
		writeWR.setSend_flags(0);
		LinkedList<IbvSge> sgeListWrite = new LinkedList<>();
		IbvSge sgeSendWrite = new IbvSge();
		sgeListWrite.add(sgeSendWrite);
		writeWR.setSg_list(sgeListWrite);
		wrList_send.add(writeWR);
	
		IbvSendWR readWR = new IbvSendWR();
		readWR.setWr_id(opcount.getAndIncrement());
		readWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
		readWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
		
		LinkedList<IbvSge> sgeListRead = new LinkedList<>();
		IbvSge sgeSendRead = new IbvSge();
		sgeSendRead.setLength(1);
		sgeListRead.add(sgeSendRead);
		readWR.setSg_list(sgeListRead);
		wrList_send.add(readWR);
		
		SVCPostSend rdmaOp = this.postSend(wrList_send);
		
		return rdmaOp;
	}

	private SVCPostSend initReadOp() throws IOException{
		LinkedList<IbvSendWR> wrList_send = new LinkedList<>();
		LinkedList<IbvSge> sgeList = new LinkedList<>();
		IbvSge sgeSend = new IbvSge();
		IbvSendWR sendWR = new IbvSendWR();
		
		sgeList.add(sgeSend);
		sendWR.setSg_list(sgeList);
		wrList_send.add(sendWR);			
		
		sendWR.setWr_id(opcount.getAndIncrement());
		sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);

		return this.postSend(wrList_send);
	}
	
	public DatanodeFuture write(DatagridBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException, InterruptedException {
		if (buffer.remaining() > DatagridConstants.BLOCK_SIZE) {
			throw new IOException("write size too large " + buffer.remaining());
		}
		if (buffer.remaining() <= 0) {
			throw new IOException("write size too small, len " + buffer.remaining());
		}	
		if (buffer.position() < 0) {
			throw new IOException("local offset too small " + buffer.position());
		}
		if (remoteOffset < 0) {
			throw new IOException("remote offset too small " + remoteOffset);
		}	
		if (remoteMr.getAddr() == 0) {
			throw new IOException("remote addr is 0 " + remoteMr.getAddr());
		}		
		if (remoteMr.getLkey() == 0) {
			throw new IOException("remote key is 0 " + remoteMr.getLkey());
		}	
		
		if (deviceCache == null) {
			deviceCache = mrCache.getDeviceCache(this.getPd());
		}
		IbvMr localMr = deviceCache.get(buffer.getRegion());
		if (localMr == null) {
			localMr = this.registerMemory(buffer.getRegion().getByteBuffer()).execute().free().getMr();
			deviceCache.put(localMr);
		}
		long bufferAddress = buffer.address();
		
		SVCPostSend writeOp = writeOps.take();
		
		SendWRMod sendWriteWR = writeOp.getWrMod(0);
		sendWriteWR.setWr_id(opcount.getAndIncrement());
		sendWriteWR.getRdmaMod().setRemote_addr(remoteMr.getAddr() + remoteOffset);
		sendWriteWR.getRdmaMod().setRkey(remoteMr.getLkey());			
		
		SgeMod sgeSendWrite = writeOp.getWrMod(0).getSgeMod(0);
		sgeSendWrite.setAddr(bufferAddress + buffer.position());
		sgeSendWrite.setLength(buffer.remaining());
		sgeSendWrite.setLkey(localMr.getLkey());

		SendWRMod sendReadWR = writeOp.getWrMod(1);
		sendReadWR.setWr_id(opcount.getAndIncrement());
		sendReadWR.getRdmaMod().setRemote_addr(remoteMr.getAddr() + remoteOffset);
		sendReadWR.getRdmaMod().setRkey(remoteMr.getLkey());			
		
		SgeMod sgeSendRead = writeOp.getWrMod(1).getSgeMod(0);
		sgeSendRead.setAddr(bufferAddress + buffer.position());
		sgeSendRead.setLkey(localMr.getLkey());

		while (!sendQueueAvailable.tryAcquire()) {
			this.pollOnce();
		}	
		while (!sendQueueAvailable.tryAcquire()) {
			this.pollOnce();
		}			
		
		RdmaPassiveFuture future = new RdmaPassiveFuture(this, sendReadWR.getWr_id(), sgeSendWrite.getLength(), true);
		
		futureMap.put(future.getWrid(), future);	
		writeOp.execute();
		writeOps.add(writeOp);
		
		return future;
	}

	public DatanodeFuture read(DatagridBuffer buffer, BlockInfo remoteMr, long remoteOffset) throws IOException, InterruptedException {
		if (buffer.remaining() > DatagridConstants.BLOCK_SIZE) {
			throw new IOException("read size too large");
		}	
		if (buffer.remaining() <= 0) {
			throw new IOException("read size too small, len " + buffer.remaining());
		}
		if (buffer.position() < 0) {
			throw new IOException("local offset too small " + buffer.position());
		}
		if (remoteOffset < 0) {
			throw new IOException("remote offset too small " + remoteOffset);
		}
		if (remoteMr.getAddr() == 0) {
			throw new IOException("remote addr is 0 " + remoteMr.getAddr());
		}		
		if (remoteMr.getLkey() == 0) {
			throw new IOException("remote key is 0 " + remoteMr.getLkey());
		}
		
		if (deviceCache == null) {
			deviceCache = mrCache.getDeviceCache(this.getPd());
		}
		IbvMr localMr = deviceCache.get(buffer.getRegion());
		if (localMr == null) {
			localMr = this.registerMemory(buffer.getRegion().getByteBuffer()).execute().free().getMr();
			deviceCache.put(localMr);
		}
		long bufferAddress = buffer.address();
		
		SVCPostSend readOp = readOps.take();
		
		SendWRMod sendWR = readOp.getWrMod(0);
		sendWR.setWr_id(opcount.getAndIncrement());
		SgeMod sgeSend = sendWR.getSgeMod(0);
		sgeSend.setAddr(bufferAddress + buffer.position());
		sgeSend.setLength(buffer.remaining());
		sgeSend.setLkey(localMr.getLkey());
		
		sendWR.getRdmaMod().setRemote_addr(remoteMr.getAddr() + remoteOffset);
		sendWR.getRdmaMod().setRkey(remoteMr.getLkey());
		
		while (!sendQueueAvailable.tryAcquire()) {
			this.pollOnce();
		}
		
		RdmaPassiveFuture future = new RdmaPassiveFuture(this, sendWR.getWr_id(), sgeSend.getLength(), false);
		
		futureMap.put(future.getWrid(), future);
		readOp.execute();
		readOps.add(readOp);
		
		return future;		
	}
	
	public int pollOnce() throws IOException {
		int res = 0;
		if (!lock.tryLock()) {
//			logger.info("cannot get lock on ep " + this.getEndpointId());
			return res;
		}
		
//		logger.info("got lock on ep " + this.getEndpointId());
		try {
			res = _pollOnce();
//			while (_poll() <= 0);
		} finally {
			lock.unlock();
		}
		return res;
	}	
	
	public void pollUntil(AtomicInteger future, long timeout) throws IOException {
		boolean locked = false;
		while (true) {
			locked = lock.tryLock();
			if (future.get() > 0 || locked) {
				break;
			}
		}

		try {
			if (future.get() == 0) {
				_pollUntil(future, timeout);
			}
		} finally {
			if (locked) {
				lock.unlock();
			}
		}
	}	
	
	private int _pollOnce() throws IOException {
		int res = poll.execute().getPolls();
		if (res > 0) {
			for (int i = 0; i < res; i++) {
				IbvWC wc = wcList[i];
				dispatchCqEvent(wc);
			}
			
		} 
		return res;
	}	
	
	private int _pollUntil(AtomicInteger future, long timeout) throws IOException {
		long count = 0;
		final long checkTimeOut = 1 << 14 /* 16384 */;
		long startTime = System.nanoTime();
		while (future.get() == 0) {
			int res = poll.execute().getPolls();
			if (res > 0) {
				for (int i = 0; i < res; i++) {
					IbvWC wc = wcList[i];
					dispatchCqEvent(wc);
				}
			}
			if (count == checkTimeOut) {
				count = 0;
				if ((System.nanoTime() - startTime) / 1e6 > timeout) {
					break;
				}
			}			
			count++;
		}
		return 1;
	}
	
	private void dispatchCqEvent(IbvWC wc) throws IOException {
		if (wc.getStatus() == 5) {
//			logger.info("flush wc");
		} else if (wc.getStatus() != 0) {
			logger.info("faulty request, status " + wc.getStatus());
		} else {
			RdmaPassiveFuture future = futureMap.remove(wc.getWr_id());
			if (future != null) {
				future.signal(wc.getStatus());
				if (future.isWrite()) {
					sendQueueAvailable.release(2);
				} else {
					sendQueueAvailable.release();
				}
			} else {
				throw new IOException("cannot find future object for wrid " + wc.getWr_id() + ", status " + wc.getStatus() + ", opcount " + opcount + ", ep " + this.getEndpointId() + ", wc.qpnum " + wc.getQp_num() + ", this.qp.num " + this.qp.getQp_num() + ", connstate " + this.getConnState() + ", futureMap.size " + futureMap.size());
			}
		}
	}

	@Override
	public void close() throws IOException, InterruptedException {
		lock.lock();
		try {
			while (pollOnce() > 0);
		} finally {
			lock.unlock();
		}		
		super.close();
	}
	
	public int getFreeSlots() {
		return this.sendQueueAvailable.availablePermits();
	}
	
	public String getAddress() throws IOException {
		return super.getDstAddr().toString();
	}
	
	public RdmaCmId getContext() {
		return super.getIdPriv();
	}

	@Override
	public boolean isLocal() {
		return false;
	}	
}