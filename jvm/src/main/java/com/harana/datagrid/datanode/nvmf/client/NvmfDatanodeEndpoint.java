package com.harana.datagrid.datanode.nvmf.client;

import com.harana.datagrid.DatagridStatistics;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.client.datanode.DatanodeFuture;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.datanode.nvmf.jvnmf.*;
import com.harana.datagrid.DatagridBuffer;
import com.harana.datagrid.DatagridBufferCache;
import com.harana.datagrid.metadata.BlockInfo;
import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.datanode.nvmf.NvmfStorageConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class NvmfDatanodeEndpoint implements DatanodeEndpoint {
	private static final Logger logger = LogManager.getLogger();

	private final Controller controller;
	private final IoQueuePair queuePair;
	private final int lbaDataSize;
	private final long namespaceCapacity;
	private final NvmfRegisteredBufferCache registeredBufferCache;
	private final NvmfStagingBufferCache stagingBufferCache;
	private final DatagridStatistics statistics;

	private final Queue<NvmWriteCommand> writeCommands;
	private final Queue<NvmReadCommand> readCommands;

	private final AtomicInteger outstandingOperations;

	public NvmfDatanodeEndpoint(Nvme nvme, DatanodeInfo info, DatagridStatistics statistics, DatagridBufferCache bufferCache) throws IOException {
		InetSocketAddress inetSocketAddress = new InetSocketAddress(
				InetAddress.getByAddress(info.getIpAddress()), info.getPort());
		// XXX FIXME: nsid from datanodeinfo
		NvmfTransportId transportId = new NvmfTransportId(inetSocketAddress, new NvmeQualifiedName(NvmfStorageConstants.NQN.toString()));
		logger.info("Connecting to NVMf target at " + transportId.toString());
		controller = nvme.connect(transportId);
		controller.getControllerConfiguration().setEnable(true);
		controller.syncConfiguration();
		try {
			controller.waitUntilReady();
		} catch (TimeoutException e) {
			throw new IOException(e);
		}
		IdentifyControllerData identifyControllerData = controller.getIdentifyControllerData();
		if (DatagridConstants.SLICE_SIZE > identifyControllerData.getMaximumDataTransferSize().toInt()) {
			throw new IllegalArgumentException(DatagridConstants.SLICE_SIZE_KEY + " > max transfer size (" + identifyControllerData.getMaximumDataTransferSize() + ")");
		}
		List<Namespace> namespaces = controller.getActiveNamespaces();
		//TODO: poll nsid in datanodeinfo
		NamespaceIdentifier namespaceIdentifier = new NamespaceIdentifier(1);
		Namespace namespace = null;
		for (Namespace n : namespaces) {
			if (n.getIdentifier().equals(namespaceIdentifier)) {
				namespace = n;
				break;
			}
		}
		if (namespace == null) {
			throw new IllegalArgumentException("No namespace with id " + namespaceIdentifier + " at controller " + transportId.toString());
		}
		IdentifyNamespaceData identifyNamespaceData = namespace.getIdentifyNamespaceData();
		lbaDataSize = identifyNamespaceData.getFormattedLbaSize().getLbaDataSize().toInt();
		if (DatagridConstants.SLICE_SIZE % lbaDataSize != 0) {
			throw new IllegalArgumentException(DatagridConstants.SLICE_SIZE_KEY + " is not a multiple of LBA data size (" + lbaDataSize + ")");
		}
		namespaceCapacity = identifyNamespaceData.getNamespaceCapacity() * lbaDataSize;
		this.queuePair = controller.createIoQueuePair(NvmfStorageConstants.QUEUE_SIZE, 0, 0, SubmissionQueueEntry.SIZE);

		this.writeCommands = new ArrayBlockingQueue<>(NvmfStorageConstants.QUEUE_SIZE);
		this.readCommands = new ArrayBlockingQueue<>(NvmfStorageConstants.QUEUE_SIZE);
		for(int i = 0; i < NvmfStorageConstants.QUEUE_SIZE; i++) {
			NvmWriteCommand writeCommand = new NvmWriteCommand(queuePair);
			writeCommand.setSendInline(true);
			writeCommand.getCommandCapsule().getSubmissionQueueEntry().setNamespaceIdentifier(namespaceIdentifier);
			writeCommands.add(writeCommand);
			NvmReadCommand readCommand = new NvmReadCommand(queuePair);
			readCommand.setSendInline(true);
			readCommand.getCommandCapsule().getSubmissionQueueEntry().setNamespaceIdentifier(namespaceIdentifier);
			readCommands.add(readCommand);
		}
		this.registeredBufferCache = new NvmfRegisteredBufferCache(queuePair);
		this.outstandingOperations = new AtomicInteger(0);
		this.stagingBufferCache = new NvmfStagingBufferCache(bufferCache,
				NvmfStorageConstants.STAGING_CACHE_SIZE, getLBADataSize());
		this.statistics = statistics;
	}

	public void keepAlive() throws IOException {
		controller.keepAlive();
	}

	public int getLBADataSize() {
		return lbaDataSize;
	}

	public long getNamespaceCapacity() {
		return namespaceCapacity;
	}

	enum Operation {
		WRITE,
		READ
	}

	void putOperation() {
		outstandingOperations.decrementAndGet();
	}

	private boolean tryGetOperation() {
		int outstandingOperationsOld = outstandingOperations.get();
		if (outstandingOperationsOld < NvmfStorageConstants.QUEUE_SIZE) {
			return outstandingOperations.compareAndSet(outstandingOperationsOld, outstandingOperationsOld + 1);
		}
		return false;
	}

	private static int divCeil(int a, int b) {
		return (a + b - 1) / b;
	}

	private int getNumLogicalBlocks(DatagridBuffer buffer) {
		return divCeil(buffer.remaining(), getLBADataSize());
	}

	DatanodeFuture Op(Operation op, DatagridBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws IOException {
		assert blockInfo.getAddr() + remoteOffset + buffer.remaining() <= getNamespaceCapacity();
		assert remoteOffset >= 0;
		assert buffer.remaining() <= DatagridConstants.BLOCK_SIZE;

		long startingAddress = blockInfo.getAddr() + remoteOffset;
		if (startingAddress % getLBADataSize() != 0 ||
				((startingAddress + buffer.remaining()) % getLBADataSize() != 0 && op == Operation.WRITE)) {
			if (op == Operation.READ) {
				throw new IOException("Unaligned read access is not supported. Address (" + startingAddress +
						") needs to be multiple of LBA data size " + getLBADataSize());
			}
			try {
				return new NvmfUnalignedWriteFuture(this, buffer, blockInfo, remoteOffset);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}

		if (!tryGetOperation()) {
			do {
				poll();
			} while (!tryGetOperation());
		}

		NvmIoCommand<? extends NvmIoCommandCapsule> command;
		NvmfDatanodeFuture<?> future;
		Response<NvmResponseCapsule> response;
		if (op == Operation.READ) {
			NvmReadCommand readCommand = readCommands.remove();
			response = readCommand.newResponse();
			future = new NvmfDatanodeFuture<>(this, readCommand, response, readCommands, buffer.remaining());
			command = readCommand;
		} else {
			NvmWriteCommand writeCommand = writeCommands.remove();
			response = writeCommand.newResponse();
			future = new NvmfDatanodeFuture<>(this, writeCommand, response, writeCommands, buffer.remaining());
			command = writeCommand;
		}
		command.setCallback(future);
		response.setCallback(future);

		NvmIoCommandSqe sqe = command.getCommandCapsule().getSubmissionQueueEntry();
		long startingLBA = startingAddress / getLBADataSize();
		sqe.setStartingLba(startingLBA);
		/* TODO: on read this potentially overwrites data beyond the set limit */
		int numLogicalBlocks = getNumLogicalBlocks(buffer);
		buffer.limit(buffer.position() + numLogicalBlocks * getLBADataSize());
		sqe.setNumberOfLogicalBlocks(numLogicalBlocks);
		int remoteKey = registeredBufferCache.getRemoteKey(buffer);
		KeyedSglDataBlockDescriptor dataBlockDescriptor = sqe.getKeyedSglDataBlockDescriptor();
		dataBlockDescriptor.setAddress(buffer.address() + buffer.position());
		dataBlockDescriptor.setLength(buffer.remaining());
		dataBlockDescriptor.setKey(remoteKey);

		command.execute(response);

		return future;
	}

	public DatanodeFuture write(DatagridBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws IOException {
		return Op(Operation.WRITE, buffer, blockInfo, remoteOffset);
	}

	public DatanodeFuture read(DatagridBuffer buffer, BlockInfo blockInfo, long remoteOffset) throws IOException {
		return Op(Operation.READ, buffer, blockInfo, remoteOffset);
	}

	void poll() throws IOException {
		queuePair.poll();
	}

	public void close() throws IOException {
		registeredBufferCache.free();
		controller.free();
	}

	public boolean isLocal() {
		return false;
	}

	NvmfStagingBufferCache getStagingBufferCache() {
		return stagingBufferCache;
	}
}