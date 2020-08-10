package com.harana.datagrid.datanode.nvmf.jvnmf;

abstract class CommandOpcode extends CommandType {

  protected CommandOpcode(boolean generic, int function, DataTransfer dataTransfer, boolean admin) {
    super(generic, function, dataTransfer, admin);
  }
}
