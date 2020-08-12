package com.harana.datagrid.datanode.nvmf.jnvmf;

import com.harana.datagrid.rdma.verbs.IbvWC;

public class RdmaException extends Exception {

  private final IbvWC.IbvWcOpcode opcode;
  private final IbvWC.IbvWcStatus status;

  RdmaException(IbvWC.IbvWcOpcode opcode, IbvWC.IbvWcStatus status) {
    super(opcode.name() + " WC status " + status.ordinal() + ": " + status.name());
    this.status = status;
    this.opcode = opcode;
  }

  public IbvWC.IbvWcStatus getStatus() {
    return this.status;
  }

  public IbvWC.IbvWcOpcode getOpcode() {
    return opcode;
  }

  public static RdmaException fromInteger(int opcode, int status) {
    return new RdmaException(IbvWC.IbvWcOpcode.valueOf(opcode), IbvWC.IbvWcStatus.valueOf(status));
  }
}
