package com.harana.datagrid.datanode.nvmf.jvnmf;

public class AdminIdentifyActiveNamespacesCommandCapsule extends AdminIdentifyCommandCapsule {

  AdminIdentifyActiveNamespacesCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, AdminIdentifyCommandReturnType.getInstance().ACTIVE_NAMESPACE_IDS);
  }

  public void setSglDescriptor(NamespaceIdentifierList data) {
    KeyedSglDataBlockDescriptor keyedSglDataBlockDescriptor =
        getSubmissionQueueEntry().getKeyedSglDataBlockDescriptor();
    keyedSglDataBlockDescriptor.set(data.getBuffer());
  }
}
