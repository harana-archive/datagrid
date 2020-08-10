package com.harana.datagrid.datanode.nvmf.jvnmf;

public class AdminAsynchronousEventRequestCommandSqe extends AdminSubmissionQeueueEntry {
  /*
   * NVMe Spec 1.3a - 5.2
   *
   */

  AdminAsynchronousEventRequestCommandSqe(NativeBuffer buffer) {
    super(buffer);
  }

  @Override
  void initialize() {
    super.initialize();
    setOpcode(AdminCommandOpcode.ASYNCHRONOUS_EVENT_REQUEST);
  }
}
