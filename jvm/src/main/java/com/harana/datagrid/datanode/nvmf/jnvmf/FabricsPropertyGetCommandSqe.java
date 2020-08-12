package com.harana.datagrid.datanode.nvmf.jnvmf;

public class FabricsPropertyGetCommandSqe extends FabricsPropertyCommandSqe {

  FabricsPropertyGetCommandSqe(NativeBuffer buffer) {
    super(buffer);
  }

  @Override
  FabricsCommandType getCommandType() {
    return FabricsCommandType.PROPERTY_GET;
  }
}
