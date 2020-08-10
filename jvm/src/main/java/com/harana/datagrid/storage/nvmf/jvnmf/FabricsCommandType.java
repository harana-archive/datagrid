package com.harana.datagrid.storage.nvmf.jvnmf;

class FabricsCommandType extends CommandType {
  /*
   * NVMf Spec 1.0 - 3
   */

  static final FabricsCommandType PROPERTY_SET = new FabricsCommandType(false, 0, DataTransfer.NO,
      true);
  static final FabricsCommandType CONNECT = new FabricsCommandType(false, 0,
      DataTransfer.HOST_TO_CONTROLLER, false);
  static final FabricsCommandType PROPERTY_GET = new FabricsCommandType(false, 1, DataTransfer.NO,
      true);

  private FabricsCommandType(boolean generic, int function, DataTransfer dataTransfer,
      boolean admin) {
    super(generic, function, dataTransfer, admin);
  }
}
