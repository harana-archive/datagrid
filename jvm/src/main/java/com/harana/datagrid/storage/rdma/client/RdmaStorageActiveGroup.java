package com.harana.datagrid.storage.rdma.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.storage.StorageEndpoint;
import com.harana.datagrid.storage.rdma.MrCache;
import com.harana.datagrid.storage.rdma.RdmaConstants;
import com.harana.datagrid.storage.rdma.RdmaStorageGroup;
import com.harana.datagrid.rdma.*;
import com.harana.datagrid.utils.CrailUtils;

public class RdmaStorageActiveGroup extends RdmaActiveEndpointGroup<RdmaStorageActiveEndpoint> implements RdmaStorageGroup {
	private HashMap<InetSocketAddress, RdmaStorageLocalEndpoint> localCache;
	private MrCache mrCache;
	
	public RdmaStorageActiveGroup(int timeout, boolean polling, int maxWR, int maxSge, int cqSize, MrCache mrCache) throws IOException {
		super(timeout, polling, maxWR, maxSge, cqSize);
		try {
			this.mrCache = mrCache;
			this.localCache = new HashMap<>();
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
		try {
			return createEndpoint(CrailUtils.datanodeInfo2SocketAddr(info));
		} catch(Exception e){
			throw new IOException(e);
		}
	}

	//	@Override
	public StorageEndpoint createEndpoint(InetSocketAddress inetAddress) throws Exception {
		if (RdmaConstants.STORAGE_RDMA_LOCAL_MAP && CrailUtils.isLocalAddress(inetAddress.getAddress())){
			RdmaStorageLocalEndpoint localEndpoint = localCache.get(inetAddress.getAddress());
			if (localEndpoint == null){
				localEndpoint = new RdmaStorageLocalEndpoint(inetAddress);
				localCache.put(inetAddress, localEndpoint);
			}
			return localEndpoint;			
		}
		
		RdmaStorageActiveEndpoint endpoint = super.createEndpoint();
		endpoint.connect(inetAddress, RdmaConstants.STORAGE_RDMA_CONNECTTIMEOUT);
		return endpoint;
	}

	public int getType() {
		return 0;
	}

	@Override
	public String toString() {
		return "maxWR " + getMaxWR() + ", maxSge " + getMaxSge() + ", cqSize " + getCqSize();
	}

	public MrCache getMrCache() {
		return mrCache;
	}
}
