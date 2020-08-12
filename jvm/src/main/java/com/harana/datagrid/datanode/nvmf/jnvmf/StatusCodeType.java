package com.harana.datagrid.datanode.nvmf.jnvmf;

public class StatusCodeType extends EEnum<StatusCodeType.Value> {

  public class Value extends EEnum.Value {

    private final String description;
    private final StatusCode nvmStatusCode;
    private final StatusCode fabricsStatusCode;
    private final StatusCode adminStatusCode;

    Value(int value, String description, StatusCode nvmStatusCode, StatusCode fabricsStatusCode,
        StatusCode adminStatusCode) {
      super(value);
      this.description = description;
      this.nvmStatusCode = nvmStatusCode;
      this.fabricsStatusCode = fabricsStatusCode;
      this.adminStatusCode = adminStatusCode;
    }

    StatusCode.Value nvmValueOf(int value) {
      return nvmStatusCode.valueOf(value);
    }

    StatusCode.Value fabricsValueOf(int value) {
      return fabricsStatusCode.valueOf(value);
    }

    StatusCode.Value adminValueOf(int value) {
      return adminStatusCode.valueOf(value);
    }

    public String getDescription() {
      return description;
    }
  }

  // CHECKSTYLE_OFF: MemberNameCheck

  final Value GENERIC = new Value(0x0, "Generic",
      NvmGenericStatusCode.getInstance(),
      GenericStatusCode.getInstance(),
      GenericStatusCode.getInstance());
  final Value COMMAND_SPECIFIC = new Value(0x1, "Command Specific",
      NvmCommandStatusCode.getInstance(),
      FabricsCommandStatusCode.getInstance(),
      null);
  final Value MEDIA_ERROR = new Value(0x2, "Media and Data Integrity Error",
      null, null, null);
  //TODO: missing values
  final Value VENDOR_SPECIFIC = new Value(0x7, "Vendor Specific",
      null, null, null);

  // CHECKSTYLE_ON: MemberNameCheck

  private StatusCodeType() {
    super(7);
  }

  private static final StatusCodeType instance = new StatusCodeType();

  public static StatusCodeType getInstance() {
    return instance;
  }
}
