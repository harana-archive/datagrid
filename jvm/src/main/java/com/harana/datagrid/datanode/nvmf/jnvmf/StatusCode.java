package com.harana.datagrid.datanode.nvmf.jnvmf;

public abstract class StatusCode extends EEnum<StatusCode.Value> {

  public class Value extends EEnum.Value {

    private final String description;

    Value(int value, String description) {
      super(value);
      this.description = description;
    }

    public String getDescription() {
      return description;
    }
  }

  StatusCode() {
    super(0xff);
  }
}
