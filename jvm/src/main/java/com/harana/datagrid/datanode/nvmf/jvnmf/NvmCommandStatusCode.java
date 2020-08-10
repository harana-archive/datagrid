package com.harana.datagrid.datanode.nvmf.jvnmf;

public class NvmCommandStatusCode extends CommandSpecificStatusCode {

  public class Value extends CommandSpecificStatusCode.Value {

    Value(int value, String description) {
      super(value, description);
    }
  }

  // CHECKSTYLE_OFF: MemberNameCheck

  /* NVM Spec 1.3a - 4.6.1.2.2 Figure 34 */
  public final Value CONFLICTING_ATTRIBUTES = new Value(0x80,
      "The attributes specified in the command are conflicting.");
  public final Value INVALID_PROTECTION_INFORMATION = new Value(0x81,
      "The Protection Information Field (PRINFO) "
          + "settings specified in the command are invalid for the Protection Information with "
          + "which the namespace was formatted or the EILBRT/ILBRT field is invalid.");
  public final Value ATTEMTED_WRITE_TO_READ_ONLY_RANGE = new Value(0x82,
      "The LBA range specified contains read-only blocks.");

  // CHECKSTYLE_ON: MemberNameCheck

  private NvmCommandStatusCode() {
  }

  private static final NvmCommandStatusCode instance = new NvmCommandStatusCode();

  public static NvmCommandStatusCode getInstance() {
    return instance;
  }
}
