package com.harana.datagrid.datanode.nvmf.jnvmf;

public class NvmCommand<C extends NvmCommandCapsule> extends Command<C, NvmResponseCapsule> {

  NvmCommand(IoQueuePair queuePair, C command) {
    super(queuePair, command);
  }

  @Override
  public Response<NvmResponseCapsule> newResponse() {
    return new Response<>(new NvmResponseCapsule());
  }
}
