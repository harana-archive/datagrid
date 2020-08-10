package com.harana.datagrid.storage.nvmf.jvnmf;

public class AdminIdentifyNamespaceCommandCapsule extends AdminIdentifyCommandCapsule {

  AdminIdentifyNamespaceCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, AdminIdentifyCommandReturnType.getInstance().NAMESPACE);
  }

  public void setSglDescriptor(IdentifyNamespaceData data) {
    KeyedSglDataBlockDescriptor keyedSglDataBlockDescriptor =
        getSubmissionQueueEntry().getKeyedSglDataBlockDescriptor();
    keyedSglDataBlockDescriptor.set(data.getBuffer());
  }
}
