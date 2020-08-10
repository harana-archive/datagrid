package com.harana.datagrid.datanode.nvmf.jvnmf;

public class NvmReadCommandSqe extends NvmIoCommandSqe {

  NvmReadCommandSqe(NativeBuffer buffer) {
    super(buffer);
  }

  @Override
  void initialize() {
    super.initialize();
    setOpcode(NvmCommandOpcode.READ);
  }
}