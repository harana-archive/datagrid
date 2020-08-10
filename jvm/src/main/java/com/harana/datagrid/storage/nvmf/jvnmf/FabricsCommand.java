package com.harana.datagrid.storage.nvmf.jvnmf;

public abstract class FabricsCommand<C extends CommandCapsule> extends
    Command<C, FabricsResponseCapsule> {

  FabricsCommand(QueuePair queuePair, C command) {
    super(queuePair, command);
  }

  @Override
  public Response<FabricsResponseCapsule> newResponse() {
    return new Response<>(new FabricsResponseCapsule());
  }
}
