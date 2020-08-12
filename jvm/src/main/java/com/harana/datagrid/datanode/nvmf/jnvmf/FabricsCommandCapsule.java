package com.harana.datagrid.datanode.nvmf.jnvmf;

public class FabricsCommandCapsule<SqeT extends FabricsSubmissionQueueEntry>
    extends CommandCapsule<SqeT> {

  FabricsCommandCapsule(KeyedNativeBuffer buffer, SubmissionQueueEntryFactory<SqeT> sqeFactory) {
    super(buffer, sqeFactory);
  }
}
