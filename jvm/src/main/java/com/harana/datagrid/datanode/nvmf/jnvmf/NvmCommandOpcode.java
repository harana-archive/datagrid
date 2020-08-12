package com.harana.datagrid.datanode.nvmf.jnvmf;

public class NvmCommandOpcode extends CommandOpcode {
  /*
   * NVMe Spec 1.3a - 6
   */

  static final NvmCommandOpcode FLUSH = new NvmCommandOpcode(false, 0,
          DataTransfer.NO);
  static final NvmCommandOpcode WRITE = new NvmCommandOpcode(false, 0,
      DataTransfer.HOST_TO_CONTROLLER);
  static final NvmCommandOpcode READ = new NvmCommandOpcode(false, 0,
      DataTransfer.CONTROLLER_TO_HOST);

  protected NvmCommandOpcode(boolean generic, int function, DataTransfer dataTransfer) {
    super(generic, function, dataTransfer, false);
  }
}
