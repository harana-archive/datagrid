package com.harana.datagrid.datanode.nvmf.jnvmf;

public class NvmFlushCommandSqe extends NvmSubmissionQueueEntry {
  NvmFlushCommandSqe(NativeBuffer buffer) {
    super(buffer);
  }

  @Override
  void initialize() {
    super.initialize();
    setOpcode(NvmCommandOpcode.FLUSH);
  }
}
