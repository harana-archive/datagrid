package com.harana.datagrid.datanode.nvmf.jnvmf;

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
