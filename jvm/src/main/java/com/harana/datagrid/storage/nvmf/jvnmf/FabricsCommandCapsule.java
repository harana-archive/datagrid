package com.harana.datagrid.storage.nvmf.jvnmf;

public class FabricsCommandCapsule<SqeT extends FabricsSubmissionQueueEntry>
    extends CommandCapsule<SqeT> {

  FabricsCommandCapsule(KeyedNativeBuffer buffer, SubmissionQueueEntryFactory<SqeT> sqeFactory) {
    super(buffer, sqeFactory);
  }
}
