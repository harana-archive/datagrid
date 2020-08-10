package com.harana.datagrid.datanode.nvmf.jvnmf;

public class FabricsCommandOpcode extends CommandOpcode {

  /*
   * NVMf Spec 1.0 - 2.1
   *
   * Set to 0x7F to indicate Fabrics command
   */
  public static final FabricsCommandOpcode FABRIC =
      new FabricsCommandOpcode(false, 0x1F, DataTransfer.BIDIRECTIONAL, false);

  protected FabricsCommandOpcode(boolean generic, int function, DataTransfer dataTransfer,
      boolean admin) {
    super(generic, function, dataTransfer, admin);
  }
}
