package com.harana.datagrid.datanode.nvmf.jvnmf;

public abstract class SubmissionQueueEntry extends NativeData<NativeBuffer> {
  /*
   * NVMe Spec 1.3a - 4.2 Submission Queue Entry
   * NVMf Spec 1.0 - 2.1
   *
   *   31:16 CommandCapsule Identifier (CID) - unique identifier for cmd together with SQ identifier
   *   08:15 NVMf/e specific/*
   *   07:00 CommandType (OPC)
   *
   */

  public static final int SIZE = 64;

  private static final int OPCODE_OFFSET = 0;
  private static final int COMMAND_IDENTIFIER_OFFSET = 2;
  private static final int SGL_DESCRIPTOR1_OFFSET = 24;

  SubmissionQueueEntry(NativeBuffer buffer) {
    super(buffer, SIZE);
  }

  final void setOpcode(CommandType opcode) {
    getBuffer().put(OPCODE_OFFSET, opcode.toByte());
  }

  final NativeBuffer getSglDescriptor1Buffer() {
    getBuffer().position(SGL_DESCRIPTOR1_OFFSET);
    getBuffer().limit(getBuffer().position() + ScatterGatherListDescriptor.SIZE);
    NativeBuffer sglDescriptorBuffer = getBuffer().slice();
    getBuffer().clear();
    return sglDescriptorBuffer;
  }

  final void setCommandIdentifier(short commandIdentifier) {
    getBuffer().putShort(COMMAND_IDENTIFIER_OFFSET, commandIdentifier);
  }

  @Override
  void initialize() {
    if (LegacySupport.ENABLED) {
      LegacySupport.initializeSubmissionQueueEntry(getBuffer());
    }
  }
}
