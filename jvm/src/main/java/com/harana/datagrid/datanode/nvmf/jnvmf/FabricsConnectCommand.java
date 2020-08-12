package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.io.IOException;

public class FabricsConnectCommand extends
    Command<FabricsConnectCommandCapsule, FabricsConnectResponseCapsule> {

  FabricsConnectCommand(QueuePair queuePair) throws IOException {
    super(queuePair, new FabricsConnectCommandCapsule(queuePair.allocateCommandCapsule()));
  }

  @Override
  public Response<FabricsConnectResponseCapsule> newResponse() {
    return new Response<>(new FabricsConnectResponseCapsule());
  }
}
