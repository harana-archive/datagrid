package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.io.IOException;

public class FabricsPropertyGetCommand extends
    Command<FabricsPropertyGetCommandCapsule, FabricsPropertyGetResponseCapsule> {

  FabricsPropertyGetCommand(QueuePair queuePair) throws IOException {
    super(queuePair, new FabricsPropertyGetCommandCapsule(queuePair.allocateCommandCapsule()));
  }

  @Override
  public Response<FabricsPropertyGetResponseCapsule> newResponse() {
    return new Response<>(new FabricsPropertyGetResponseCapsule());
  }
}
