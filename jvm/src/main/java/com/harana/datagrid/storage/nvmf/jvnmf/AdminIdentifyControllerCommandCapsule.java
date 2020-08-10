package com.harana.datagrid.storage.nvmf.jvnmf;

public class AdminIdentifyControllerCommandCapsule extends AdminIdentifyCommandCapsule {

  AdminIdentifyControllerCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, AdminIdentifyCommandReturnType.getInstance().CONTROLLER);
  }

  public void setSglDescriptor(IdentifyControllerData data) {
    KeyedSglDataBlockDescriptor keyedSglDataBlockDescriptor =
        getSubmissionQueueEntry().getKeyedSglDataBlockDescriptor();
    keyedSglDataBlockDescriptor.set(data.getBuffer());
  }
}
