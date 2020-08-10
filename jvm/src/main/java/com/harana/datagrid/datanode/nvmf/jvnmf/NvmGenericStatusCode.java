package com.harana.datagrid.datanode.nvmf.jvnmf;

public class NvmGenericStatusCode extends GenericStatusCode {

  public class Value extends GenericStatusCode.Value {

    Value(int value, String description) {
      super(value, description);
    }
  }

  // CHECKSTYLE_OFF: MemberNameCheck

  public final Value LBA_OUT_OF_RANGE = new Value(0x80,
      "The command references an LBA that exceeds the size of the namespace.");
  public final Value CAPACITY_EXCEEDED = new Value(0x81,
      "Execution of the command has caused the capacity of the namespace to be exceeded.");
  public final Value NAMESPACE_NOT_READY = new Value(0x82,
      "The namespace is not ready to be accessed. The Do Not Retry bit "
          + "indicates whether re-issuing the command at a later time may succeed.");
  public final Value RESERVATION_CONFLICT = new Value(0x83,
      "The command was aborted due to a conflict with a reservation held on the "
          + "accessed namespace.");
  public final Value FORMAT_IN_PROGRESS = new Value(0x84,
      " A Format NVM command is in progress on the namespace. The Do Not Retry bit shall "
          + "be cleared to ‘0’ to indicate that the command may succeed if it is resubmitted.");

  // CHECKSTYLE_ON: MemberNameCheck


  private NvmGenericStatusCode() {
  }

  private static final NvmGenericStatusCode instance = new NvmGenericStatusCode();

  public static NvmGenericStatusCode getInstance() {
    return instance;
  }
}
