package com.harana.datagrid.storage.nvmf.jvnmf;

import com.gargoylesoftware.htmlunit.javascript.host.html.DataTransfer;

class AdminCommandOpcode extends CommandOpcode {
  /*
   * NVMe Spec 1.3a - 5
   */

  static final AdminCommandOpcode GET_LOG_PAGE =
      new AdminCommandOpcode(false, 0, DataTransfer.CONTROLLER_TO_HOST);
  static final AdminCommandOpcode IDENTIFY =
      new AdminCommandOpcode(false, 1, DataTransfer.CONTROLLER_TO_HOST);
  static final AdminCommandOpcode ASYNCHRONOUS_EVENT_REQUEST =
      new AdminCommandOpcode(false, 3, DataTransfer.NO);
  static final AdminCommandOpcode KEEP_ALIVE =
      new AdminCommandOpcode(false, 6, DataTransfer.NO);


  protected AdminCommandOpcode(boolean generic, int function, DataTransfer dataTransfer) {
    super(generic, function, dataTransfer, true);
  }
}
