package com.harana.datagrid.storage.nvmf;

import com.ibm.jnvmf.*;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.storage.StorageResource;
import com.harana.datagrid.storage.StorageServer;
import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class NvmfStorageServer implements StorageServer {
	private static final Logger logger = LogManager.getLogger();

	private boolean isAlive;
	private long alignedSize;
	private long address;
	private boolean initialized = false;
	private Controller controller;

	public NvmfStorageServer() {}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws Exception {
		if (initialized) {
			throw new IOException("NvmfStorageTier already initialized");
		}
		initialized = true;
		NvmfStorageConstants.parseCmdLine(crailConfiguration, args);

		Nvme nvme;
		if (NvmfStorageConstants.HOST_NQN == null) {
			nvme = new Nvme();
		} else {
			nvme = new Nvme(NvmfStorageConstants.HOST_NQN);
		}

		NvmfTransportId transportId = new NvmfTransportId(
				new InetSocketAddress(NvmfStorageConstants.IP_ADDR, NvmfStorageConstants.PORT),
				NvmfStorageConstants.NQN);
		controller = nvme.connect(transportId);
		controller.getControllerConfiguration().setEnable(true);
		controller.syncConfiguration();
		controller.waitUntilReady();

		List<Namespace> namespaces = controller.getActiveNamespaces();
		Namespace namespace = null;
		for (Namespace n : namespaces) {
			if (n.getIdentifier().equals(NvmfStorageConstants.NAMESPACE)) {
				namespace = n;
				break;
			}
		}
		if (namespace == null) {
			throw new IllegalArgumentException("No namespace with id " + NvmfStorageConstants.NAMESPACE +
					" at controller " + transportId.toString());
		}
		IdentifyNamespaceData namespaceData = namespace.getIdentifyNamespaceData();
		LbaFormat lbaFormat = namespaceData.getFormattedLbaSize();
		int dataSize = lbaFormat.getLbaDataSize().toInt();
		long namespaceSize = dataSize * namespaceData.getNamespaceCapacity();
		alignedSize = namespaceSize - (namespaceSize % NvmfStorageConstants.ALLOCATION_SIZE);
		address = 0;

		isAlive = true;
	}

	@Override
	public void printConf(Logger log) {
		NvmfStorageConstants.printConf(log);
	}

	public void run() {
		logger.info("NnvmfStorageServer started with NVMf target " + getAddress());
		while (isAlive) {
			try {
				Thread.sleep(NvmfStorageConstants.KEEP_ALIVE_INTERVAL_MS);
				controller.keepAlive();
			} catch (Exception e) {
				e.printStackTrace();
				isAlive = false;
			}
		}
	}

	@Override
	public StorageResource allocateResource() throws Exception {
		StorageResource resource = null;

		if (alignedSize > 0){
			logger.info("new block, length " + NvmfStorageConstants.ALLOCATION_SIZE);
			logger.debug("block stag 0, address " + address + ", length " + NvmfStorageConstants.ALLOCATION_SIZE);
			alignedSize -= NvmfStorageConstants.ALLOCATION_SIZE;
			resource = StorageResource.createResource(address, NvmfStorageConstants.ALLOCATION_SIZE, 0);
			address += NvmfStorageConstants.ALLOCATION_SIZE;
		}

		return resource;
	}

	@Override
	public InetSocketAddress getAddress() {
		return new InetSocketAddress(NvmfStorageConstants.IP_ADDR, NvmfStorageConstants.PORT);
	}

	@Override
	public boolean isAlive() {
		return isAlive;
	}
}
