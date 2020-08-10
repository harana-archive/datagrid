package com.harana.datagrid.datanode.nvmf.jvnmf;

public class FabricsCommandCapsule<SqeT extends FabricsSubmissionQueueEntry>
    extends CommandCapsule<SqeT> {

  FabricsCommandCapsule(KeyedNativeBuffer buffer, SubmissionQueueEntryFactory<SqeT> sqeFactory) {
    super(buffer, sqeFactory);
  }
}
