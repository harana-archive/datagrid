package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.io.IOException;

public abstract class CommandCapsule<SqeT extends SubmissionQueueEntry> extends
    NativeData<KeyedNativeBuffer>
    implements Freeable {

  /*
   * NVMf Spec 1.0 - 2
   *
   * N-1:M Data (if present)
   * M-1:64 Additional SGLs (if present) where M-1 max ICDOFF * 16 + 64
   * 63:0 SqeT
   *
   */

  private final SqeT submissionQueueEntry;
  private final NativeBuffer data;
  private final int incapsuleDataOffset;
  private final ScatterGatherListDescriptor[] additionalSgls;

  CommandCapsule(KeyedNativeBuffer buffer, SubmissionQueueEntryFactory<SqeT> sqeFactory) {
    this(buffer, sqeFactory, 0, 0, 0);
  }

  static int computeCommandCapsuleSize(int additionalSgls, int incapsuleDataOffset,
      int incapsuleDataSize) {
    int maxCommandCapsuleSize =
        SubmissionQueueEntry.SIZE + additionalSgls * ScatterGatherListDescriptor.SIZE;
    if (incapsuleDataSize > 0) {
      try {
        maxCommandCapsuleSize = Math.max(
            Math.addExact(SubmissionQueueEntry.SIZE + incapsuleDataOffset, incapsuleDataSize),
            maxCommandCapsuleSize);
      } catch (ArithmeticException exception) {
        throw new IllegalArgumentException("Incapsule data size too large (integer overflow)");
      }
    }
    return maxCommandCapsuleSize;
  }

  CommandCapsule(KeyedNativeBuffer buffer, SubmissionQueueEntryFactory<SqeT> sqeFactory,
      int additionalSgls, int incapsuleDataOffset, int incapsuleDataSize) {
    super(buffer,
        computeCommandCapsuleSize(additionalSgls, incapsuleDataOffset, incapsuleDataSize));
    this.submissionQueueEntry = sqeFactory.construct(getBuffer());
    this.submissionQueueEntry.initialize();
    this.incapsuleDataOffset = incapsuleDataOffset;
    if (incapsuleDataSize > 0) {
      getBuffer().position(SubmissionQueueEntry.SIZE + incapsuleDataOffset);
      getBuffer().limit(getBuffer().position() + incapsuleDataSize);
      this.data = getBuffer().slice();
      getBuffer().clear();
    } else {
      this.data = null;
    }

    this.additionalSgls = new ScatterGatherListDescriptor[additionalSgls];
  }

  int getIncapsuleDataOffset() {
    return incapsuleDataOffset;
  }

  public SqeT getSubmissionQueueEntry() {
    return submissionQueueEntry;
  }

  NativeBuffer getIncapsuleData() {
    if (data == null) {
      throw new IllegalStateException("Command does not support incasule data or not enough space");
    }
    return data;
  }

  //TODO handle additional SGLs

  @Override
  void initialize() {
    submissionQueueEntry.initialize();
  }

  @Override
  KeyedNativeBuffer getBuffer() {
    return super.getBuffer();
  }

  @Override
  public void free() throws IOException {
    getBuffer().free();
  }

  @Override
  public boolean isValid() {
    return getBuffer().isValid();
  }
}
