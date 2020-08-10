package com.harana.datagrid.storage.nvmf.jvnmf;

import java.io.IOException;

public class AdminKeepAliveCommand extends
    AdminCommand<AdminKeepAliveCommandCapsule> {

  AdminKeepAliveCommand(AdminQueuePair queuePair) throws IOException {
    super(queuePair, new AdminKeepAliveCommandCapsule(queuePair.allocateCommandCapsule()));
  }
}
