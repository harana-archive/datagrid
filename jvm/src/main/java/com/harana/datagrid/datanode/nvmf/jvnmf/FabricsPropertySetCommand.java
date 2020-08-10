package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.io.IOException;

public class FabricsPropertySetCommand extends
    FabricsCommand<FabricsPropertySetCommandCapsule> {

  FabricsPropertySetCommand(AdminQueuePair queuePair) throws IOException {
    super(queuePair, new FabricsPropertySetCommandCapsule(queuePair.allocateCommandCapsule()));
  }
}
