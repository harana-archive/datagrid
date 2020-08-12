package com.harana.datagrid.datanode.nvmf.jnvmf;

public class AdminKeepAliveCommandSqe extends AdminSubmissionQeueueEntry {

  AdminKeepAliveCommandSqe(NativeBuffer buffer) {
    super(buffer);
  }

  @Override
  void initialize() {
    super.initialize();
    setOpcode(AdminCommandOpcode.KEEP_ALIVE);
  }
}
