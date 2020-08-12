package com.harana.datagrid.datanode.nvmf.jnvmf;

public class FabricsConnectResponseCapsule extends ResponseCapsule<FabricsConnectResponseCqe> {

  public FabricsConnectResponseCapsule() {
    super(new FabricsConnectResponseCqe());
  }
}
