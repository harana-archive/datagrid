package com.harana.datagrid.datanode.nvmf.jvnmf;

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