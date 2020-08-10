package com.harana.datagrid.storage.nvmf.jvnmf;

public abstract class NvmSubmissionQueueEntry extends AdminSubmissionQeueueEntry {

  private static final int FUSED_OPERATION_OFFSET = 1;

  NvmSubmissionQueueEntry(NativeBuffer buffer) {
    super(buffer);
  }

  public void setFusedOperation(NvmFusedOperation.Value fusedOperation) {
    int sndByte = getBuffer().get(FUSED_OPERATION_OFFSET);
    sndByte = sndByte | fusedOperation.toInt();
    getBuffer().put(FUSED_OPERATION_OFFSET, (byte) sndByte);
  }


  @Override
  public void setNamespaceIdentifier(NamespaceIdentifier namespaceIdentifier) {
    /* All NVM commands use the namespace identifier field */
    super.setNamespaceIdentifier(namespaceIdentifier);
  }

  @Override
  void initialize() {
    super.initialize();
    setFusedOperation(NvmFusedOperation.getInstance().NORMAL);
  }
}
