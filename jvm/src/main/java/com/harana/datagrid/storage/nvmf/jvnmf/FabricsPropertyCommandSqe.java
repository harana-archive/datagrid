package com.harana.datagrid.storage.nvmf.jvnmf;

public abstract class FabricsPropertyCommandSqe extends FabricsSubmissionQueueEntry {

  /*
   * NVMf Spec 1.0 - 3.4/3.5
   */
  private static int ATTRIBUTES_OFFSET = 40;
  private static int OFFSET_OFFSET = 44;

  FabricsPropertyCommandSqe(NativeBuffer buffer) {
    super(buffer);
  }

  public void setProperty(Property property) {
    getBuffer().put(ATTRIBUTES_OFFSET, property.getSize().toByte());
    getBuffer().putInt(OFFSET_OFFSET, property.getOffset());
  }
}
