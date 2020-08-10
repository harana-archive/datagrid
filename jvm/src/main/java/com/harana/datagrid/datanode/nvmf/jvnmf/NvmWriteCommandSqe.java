package com.harana.datagrid.datanode.nvmf.jvnmf;

public class NvmWriteCommandSqe extends NvmIoCommandSqe {

  private final SglDataBlockDescriptor sglDataBlockDescriptor;

  NvmWriteCommandSqe(NativeBuffer buffer) {
    super(buffer);
    this.sglDataBlockDescriptor = new SglDataBlockDescriptor(getSglDescriptor1Buffer());
  }

  // TODO directives

  SglDataBlockDescriptor getSglDataBlockDescriptor() {
    return sglDataBlockDescriptor;
  }

  @Override
  void initialize() {
    super.initialize();
    setOpcode(NvmCommandOpcode.WRITE);
  }
}
