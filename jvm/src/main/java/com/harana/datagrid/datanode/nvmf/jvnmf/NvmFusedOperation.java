package com.harana.datagrid.datanode.nvmf.jvnmf;

public class NvmFusedOperation extends EEnum<NvmFusedOperation.Value> {

  public class Value extends EEnum.Value {

    Value(int value) {
      super(value);
    }
  }

  // CHECKSTYLE_OFF: MemberNameCheck

  public final Value NORMAL = new Value(0x0);
  public final Value FIRST_COMMAND = new Value(0x1);
  public final Value SECOND_COMMAND = new Value(0x2);

  // CHECKSTYLE_ON: MemberNameCheck

  private NvmFusedOperation() {
    super(2);
  }

  private static final NvmFusedOperation instance = new NvmFusedOperation();

  public static NvmFusedOperation getInstance() {
    return instance;
  }
}
