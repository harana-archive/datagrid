package com.harana.datagrid.storage.nvmf.jvnmf;

public abstract class FabricsSubmissionQueueEntry extends SubmissionQueueEntry {
  /*
   * NVMf Spec 1.0 - 2.1
   *
   *  04      Fabrics CommandCapsule Type
   *  02:03   CommandCapsule Identifier (CID) - unique identifier together with SQ identifier
   *  01      Reserved
   *  0       CommandType (OPC)
   *
   */

  private static final int FABRIC_COMMAND_TYPE_OFFSET = 4;

  FabricsSubmissionQueueEntry(NativeBuffer buffer) {
    super(buffer);
  }

  private final void setFabricsCommandType(FabricsCommandType commandType) {
    getBuffer().put(FABRIC_COMMAND_TYPE_OFFSET, commandType.toByte());
  }

  abstract FabricsCommandType getCommandType();


  @Override
  void initialize() {
    super.initialize();
    setOpcode(FabricsCommandOpcode.FABRIC);
    setFabricsCommandType(getCommandType());
  }
}
