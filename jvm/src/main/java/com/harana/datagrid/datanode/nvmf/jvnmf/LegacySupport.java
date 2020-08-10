package com.harana.datagrid.datanode.nvmf.jvnmf;

public class LegacySupport {

  // FIXME: better error handling
  static final boolean ENABLED = Boolean.parseBoolean(System.getProperty("jnvmf.legacy", "false"));

  static void initializeSubmissionQueueEntry(NativeBuffer buffer) {
    /* Linux kernel up to 14.16 assumes SGL use in all commands */

    /*
     * set SGL type 01b - "SGLs are used for this transfer. If used, Metadata Pointer
     * (MPTR) contains an address of a single contiguous physical
     * buffer that is byte aligned."
     * although metadata is not supported by the kernel at the moment
     */
    buffer.put(1, (byte) (1 << 6));

    /* SGL Entry 1 -> keyed sgl data block descriptor */
    buffer.putLong(24, 0);
    buffer.putLong(32, 0);
    buffer.put(39, (byte) (0x4 << 4));
  }

  static void initializeControllerConfiguration(ControllerConfiguration controllerConfiguration) {
    /* Linux kernel up to 14.16 requires IOCQES and IOSQES to be set before admin queue is connected
     * i.e. before we know required IOCQES/IOSQES from identify controller command */
    if (controllerConfiguration.getIoCompletionQueueEntrySize().value() == 0) {
      controllerConfiguration.setIoCompletionQueueEntrySize(new QueueEntrySize(4));
    }
    if (controllerConfiguration.getIoSubmissionQueueEntrySize().value() == 0) {
      controllerConfiguration.setIoSubmissionQueueEntrySize(new QueueEntrySize(6));
    }
  }
}
