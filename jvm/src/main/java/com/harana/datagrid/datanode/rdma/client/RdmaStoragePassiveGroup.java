package com.harana.datagrid.datanode.rdma.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import com.harana.datagrid.metadata.DatanodeInfo;
import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.datanode.rdma.MrCache;
import com.harana.datagrid.datanode.rdma.RdmaConstants;
import com.harana.datagrid.datanode.rdma.RdmaDatanodeGroup;
import com.harana.datagrid.rdma.*;
import com.harana.datagrid.utils.Utils;

public class RdmaStoragePassiveGroup extends RdmaPassiveEndpointGroup<RdmaStoragePassiveEndpoint> implements RdmaDatanodeGroup {
	private final HashMap<InetSocketAddress, RdmaStorageLocalEndpoint> localCache;
	private final MrCache mrCache;

	public RdmaStoragePassiveGroup(int timeout, int maxWR, int maxSge, int cqSize, MrCache mrCache)
			throws IOException {
		super(timeout, maxWR, maxSge, cqSize);
		try {
			this.mrCache = mrCache;
			this.localCache = new HashMap<>();
		} catch(Exception e) {
			throw new IOException(e);
		}
	}
	
	public DatanodeEndpoint createEndpoint(DatanodeInfo info) throws IOException {
		try {
			return createEndpoint(Utils.datanodeInfo2SocketAddr(info));
		} catch(Exception e) {
			throw new IOException(e);
		}
	}	

	public DatanodeEndpoint createEndpoint(InetSocketAddress inetAddress) throws Exception {
		if (RdmaConstants.STORAGE_RDMA_LOCAL_MAP && Utils.isLocalAddress(inetAddress.getAddress())) {
			RdmaStorageLocalEndpoint localEndpoint = localCache.get(inetAddress.getAddress());
			if (localEndpoint == null) {
				localEndpoint = new RdmaStorageLocalEndpoint(inetAddress);
				localCache.put(inetAddress, localEndpoint);
			}
			return localEndpoint;
		} 
		
		RdmaStoragePassiveEndpoint endpoint = super.createEndpoint();
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
