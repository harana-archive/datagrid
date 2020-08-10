package com.harana.datagrid.storage.nvmf.jvnmf;

public class FabricsCommandStatusCode extends CommandSpecificStatusCode {

  public class Value extends CommandSpecificStatusCode.Value {

    Value(int value, String description) {
      super(value, description);
    }
  }

  // CHECKSTYLE_OFF: MemberNameCheck

  /* NVMf Spec 1.0 - 2.2.1 */
  public final Value CONNECT_INCOMPATIBLE_FORMAT = new Value(0x80,
      "The NVM subsystem does not support the "
          + "Connect Record Format specified by the host.");
  public final Value CONNECT_CONTROLLER_BUSY = new Value(0x81,
      "The controller is already associated with a host. This "
          + "value is also returned if there is no available controller.");
  public final Value CONNECT_INVALID_PARAMETERS = new Value(0x82,
      "One or more of the parameters (Host NQN, "
          + "Subsystem NQN, Host Identifier, Controller ID, Queue ID) specified are not valid.");
  public final Value CONNECT_RESTART_DISCOVERY = new Value(0x83,
      "The NVM subsystem requested is not available. "
          + "The host should restart the discovery process.");
  public final Value CONNECT_INVALID_HOST = new Value(0x84,
      "The host is not allowed to establish an association to any "
          + "controller in the NVM subsystem or the host is not allowed to establish an "
          + "association to the specified controller.");
  public final Value DISCOVER_RESTART = new Value(0x90,
      "The snapshot of the records is now invalid or out of date. The "
          + "host should re-read the Discovery Log Page.");
  public final Value AUTHENTICATION_REQUIRED = new Value(0x91,
      "NVMe in-band authentication is required and the "
          + "queue has not yet been authenticated.");

  // CHECKSTYLE_ON: MemberNameCheck

  private FabricsCommandStatusCode() {
  }

  private static final FabricsCommandStatusCode instance = new FabricsCommandStatusCode();

  public static FabricsCommandStatusCode getInstance() {
    return instance;
  }
}
