package com.harana.datagrid.datanode.nvmf.jvnmf;

public abstract class AdminSubmissionQeueueEntry extends SubmissionQueueEntry {

  private static int SGL_USE_OFFSET = 1;
  private static int SGL_USE_BITOFFSET = 6;
  private static int NAMESPACE_IDENTIFIER_OFFSET = 4;

  AdminSubmissionQeueueEntry(NativeBuffer buffer) {
    super(buffer);
  }

  private void setSglUse() {
    /*
     * NVMe Spec 1.3a - 4.2
     *
     * 01b for NVMf => SGLs are used for this transfer
     */
    int sndByte = getBuffer().get(SGL_USE_OFFSET);
    sndByte = BitUtil.setBit(sndByte, SGL_USE_BITOFFSET);
    getBuffer().put(SGL_USE_OFFSET, (byte) sndByte);
  }

  void setNamespaceIdentifier(NamespaceIdentifier namespaceIdentifier) {
    getBuffer().putInt(NAMESPACE_IDENTIFIER_OFFSET, namespaceIdentifier.toInt());
  }

  @Override
  void initialize() {
    super.initialize();
    setSglUse();
  }
}
