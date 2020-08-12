package com.harana.datagrid.datanode.nvmf.jnvmf;

public class FabricsConnectCommandData extends NativeData<KeyedNativeBuffer> {

  public static final int SIZE = 1024;
  private static final int HOST_IDENTIFIER_OFFSET = 0;
  private static final int CONTROLLER_ID_OFFSET = 16;
  private static final int SUBSYSTEM_NVME_QUALIFIED_NAME_OFFSET = 256;
  private static final int HOST_NVME_QUALIFIED_NAME_OFFSET = 512;

  private final NativeNvmeQualifiedName subsystemNvmeQualifiedName;
  private final NativeNvmeQualifiedName hostNvmeQualifiedName;


  FabricsConnectCommandData(KeyedNativeBuffer buffer) {
    super(buffer, SIZE);
    buffer.position(SUBSYSTEM_NVME_QUALIFIED_NAME_OFFSET);
    this.subsystemNvmeQualifiedName = new NativeNvmeQualifiedName(buffer);
    buffer.clear();
    buffer.position(HOST_NVME_QUALIFIED_NAME_OFFSET);
    this.hostNvmeQualifiedName = new NativeNvmeQualifiedName(buffer);
    buffer.clear();
  }

  void setControllerId(ControllerId controllerId) {
    getBuffer().putShort(CONTROLLER_ID_OFFSET, controllerId.toShort());
  }

  void setSubsystemNvmeQualifiedName(NvmeQualifiedName nvmeQualifiedName) {
    this.subsystemNvmeQualifiedName.set(nvmeQualifiedName);
  }

  void setHostNvmeQualifiedName(NvmeQualifiedName nvmeQualifiedName) {
    this.hostNvmeQualifiedName.set(nvmeQualifiedName);
  }

  @Override
  void initialize() {
    getBuffer().position(HOST_IDENTIFIER_OFFSET);
    FabricsHostIdentifier.getInstance().get(getBuffer());
    getBuffer().clear();
  }
}
