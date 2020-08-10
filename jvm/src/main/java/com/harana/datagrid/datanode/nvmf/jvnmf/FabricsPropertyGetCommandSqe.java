package com.harana.datagrid.datanode.nvmf.jvnmf;

public class FabricsPropertyGetCommandSqe extends FabricsPropertyCommandSqe {

  FabricsPropertyGetCommandSqe(NativeBuffer buffer) {
    super(buffer);
  }

  @Override
  FabricsCommandType getCommandType() {
    return FabricsCommandType.PROPERTY_GET;
  }
}
