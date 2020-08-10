package com.harana.datagrid.storage.nvmf.jvnmf;

import java.io.IOException;

public class AdminIdentifyActiveNamespaceIdsCommand extends
    AdminCommand<AdminIdentifyActiveNamespacesCommandCapsule> {

  AdminIdentifyActiveNamespaceIdsCommand(AdminQueuePair queuePair) throws IOException {
    super(queuePair,
        new AdminIdentifyActiveNamespacesCommandCapsule(queuePair.allocateCommandCapsule()));
  }
}
