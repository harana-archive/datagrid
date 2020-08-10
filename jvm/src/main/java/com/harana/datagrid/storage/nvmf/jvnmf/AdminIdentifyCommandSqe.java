package com.harana.datagrid.storage.nvmf.jvnmf;

public class AdminIdentifyCommandSqe extends AdminSubmissionQeueueEntry {
  /*
   * NVMe Spec 1.3a - 5.15
   *
   * Dword10:
   * 31:16 Controller identifier
   * 15:08 Reserved
   * 00:07 Controller or namepsace structure
   */

  private static final int CONTROLLER_NAMESPACE_STRUCTURE_OFFSET = 40;

  private final KeyedSglDataBlockDescriptor keyedSglDataBlockDescriptor;

  AdminIdentifyCommandSqe(NativeBuffer buffer) {
    super(buffer);
    this.keyedSglDataBlockDescriptor = new KeyedSglDataBlockDescriptor(getSglDescriptor1Buffer());
  }

  void setReturnType(AdminIdentifyCommandReturnType.Value returnType) {
    getBuffer().put(CONTROLLER_NAMESPACE_STRUCTURE_OFFSET, returnType.toByte());
  }

  /* we don't support namespace management and virtualization enhancements at the moment */
  /*void setControllerId(ControllerId controllerId) {
   }*/

  @Override
  void initialize() {
    super.initialize();
    setOpcode(AdminCommandOpcode.IDENTIFY);
  }

  KeyedSglDataBlockDescriptor getKeyedSglDataBlockDescriptor() {
    return keyedSglDataBlockDescriptor;
  }
}
