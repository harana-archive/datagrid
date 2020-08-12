package com.harana.datagrid.datanode.nvmf.jnvmf;

public class FabricsPropertySetCommandSqe extends FabricsPropertyCommandSqe {

  /*
   * NVMf Spec 1.0 - 3.5
   */
  private static int VALUE_OFFSET = 48;

  FabricsPropertySetCommandSqe(NativeBuffer buffer) {
    super(buffer);
  }

  public void setValue(long value) {
    getBuffer().putLong(VALUE_OFFSET, value);
  }

  @Override
  FabricsCommandType getCommandType() {
    return FabricsCommandType.PROPERTY_SET;
  }
}
