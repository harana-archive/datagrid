package com.harana.datagrid.datanode.nvmf.jnvmf;

public class FabricsPropertyGetResponseCqe extends FabricsCompletionQueueEntry {

  /*
   * NVMf Spec 1.0 - 3.4
   */
  private static final int VALUE_OFFSET = 0;

  private long value;

  public long getValue() {
    return value;
  }

  @Override
  void update(NativeBuffer buffer) {
    super.update(buffer);
    value = buffer.getLong(VALUE_OFFSET);
  }
}
