package com.harana.datagrid.storage.rdma;

import java.io.IOException;

import com.harana.datagrid.rdma.verbs.*;
import com.harana.datagrid.rdma.*;

public class RdmaStorageServerEndpoint extends RdmaActiveEndpoint {
	private RdmaStorageServer closer;

	public RdmaStorageServerEndpoint(RdmaActiveEndpointGroup<RdmaStorageServerEndpoint> endpointGroup, RdmaCmId idPriv, RdmaStorageServer closer, boolean serverSide) throws IOException {
		super(endpointGroup, idPriv, serverSide);
		this.closer = closer;
	}	

	public void dispatchCqEvent(IbvWC wc) throws IOException {

	}
	
	public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent)
			throws IOException {
		super.dispatchCmEvent(cmEvent);
		int eventType = cmEvent.getEvent();
		if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED
				.ordinal()) {
			closer.close(this);
		}
	}
}
