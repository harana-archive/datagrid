package com.harana.datagrid.storage.nvmf.jvnmf;

import java.io.IOException;

public class NvmWriteCommand extends NvmIoCommand<NvmWriteCommandCapsule> {

  private static NvmWriteCommandCapsule newNvmWriteCommandCapsule(IoQueuePair queuePair)
      throws IOException {
    int inCapsuleDataOffset = queuePair.getController().getIdentifyControllerData()
        .getInCapsuleDataOffset();
    return new NvmWriteCommandCapsule(queuePair.allocateCommandCapsule(),
        queuePair.getMaximumAdditionalSgls(), inCapsuleDataOffset,
        queuePair.getInCapsuleDataSize());
  }

  public NvmWriteCommand(IoQueuePair queuePair) throws IOException {
    super(queuePair, newNvmWriteCommandCapsule(queuePair));
  }

  public NativeBuffer getIncapsuleData() {
    return getCommandCapsule().getIncapsuleData();
  }

  public void setIncapsuleData(NativeBuffer incapsuleData) {
    //TODO support to set buffer outside of capsule by using RDMA sgls
    //TODO change RDMA sgl accordingly => we don't want to send unnecessary data on the wire
    getCommandCapsule().setIncapsuleData(incapsuleData);
  }
}
