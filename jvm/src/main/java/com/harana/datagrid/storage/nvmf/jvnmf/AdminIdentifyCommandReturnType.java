package com.harana.datagrid.storage.nvmf.jvnmf;

public class AdminIdentifyCommandReturnType extends EEnum<AdminIdentifyCommandReturnType.Value> {

  public class Value extends EEnum.Value {

    Value(int value) {
      super(value);
    }
  }

  // CHECKSTYLE_OFF: MemberNameCheck

  public final Value NAMESPACE = new Value(0x0);
  public final Value CONTROLLER = new Value(0x1);
  public final Value ACTIVE_NAMESPACE_IDS = new Value(0x2);
  public final Value NAMESPACE_DESCRIPTORS = new Value(0x3);
  public final Value NAMESPACE_IDS = new Value(0x10);

  // CHECKSTYLE_ON: MemberNameCheck

  private AdminIdentifyCommandReturnType() {
    super(0x15);
  }

  private static final AdminIdentifyCommandReturnType instance =
      new AdminIdentifyCommandReturnType();

  public static AdminIdentifyCommandReturnType getInstance() {
    return instance;
  }
}
