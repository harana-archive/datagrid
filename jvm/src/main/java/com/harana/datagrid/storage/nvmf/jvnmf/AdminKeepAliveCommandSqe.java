package com.harana.datagrid.storage.nvmf.jvnmf;

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
