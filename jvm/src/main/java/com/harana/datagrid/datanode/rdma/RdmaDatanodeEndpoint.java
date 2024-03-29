package com.harana.datagrid.datanode.rdma;

import java.io.IOException;

import com.harana.datagrid.rdma.verbs.*;
import com.harana.datagrid.rdma.*;

public class RdmaDatanodeEndpoint extends RdmaActiveEndpoint {
	private RdmaDatanodeServer closer;

	public RdmaDatanodeEndpoint(RdmaActiveEndpointGroup<RdmaDatanodeEndpoint> endpointGroup, RdmaCmId idPriv, RdmaDatanodeServer closer, boolean serverSide) throws IOException {
		super(endpointGroup, idPriv, serverSide);
		this.closer = closer;
	}	

	public void dispatchCqEvent(IbvWC wc) {

	}
	
	public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent) throws IOException {
		super.dispatchCmEvent(cmEvent);
		int eventType = cmEvent.getEvent();
		if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
			closer.close(this);
		}
	}
}
