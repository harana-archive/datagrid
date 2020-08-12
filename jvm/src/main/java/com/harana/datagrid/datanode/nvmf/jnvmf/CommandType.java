package com.harana.datagrid.datanode.nvmf.jnvmf;

abstract class CommandType {

  /*
   * Nvme Spec 1.3a - 5/6
   * Nvmf Spec 1.0 - 3
   *
   * 07b       Generic/Standard CommandCapsule
   * 06:02b    Function
   * 00:01b    Data Transfer
   *   00b no data transfer
   *   01b host to controller
   *   10b controller to host
   *   11b bidirectional
   */
  private static final int GENERIC_COMMAND_OFFSET = 7;
  private static final int FUNCTION_OFFSET = 2;
  private static final int DATA_TRANSFER_OFFSET = 0;

  private final byte opcode;
  private final boolean admin;

  enum DataTransfer {
    NO(0x0),
    HOST_TO_CONTROLLER(0x1),
    CONTROLLER_TO_HOST(0x2),
    BIDIRECTIONAL(0x3);

    private final int value;

    DataTransfer(int value) {
      this.value = value;
    }

    int getValue() {
      return value;
    }
  }

  protected CommandType(boolean generic, int function, DataTransfer dataTransfer, boolean admin) {
    int opcode = dataTransfer.getValue();
    opcode = opcode | (function << FUNCTION_OFFSET);
    if (generic) {
      opcode = 1 << GENERIC_COMMAND_OFFSET;
    }
    this.opcode = (byte) opcode;
    this.admin = admin;
  }

  byte toByte() {
    return opcode;
  }

  boolean adminQueueOnly() {
    return admin;
  }
}
