package com.harana.datagrid.datanode.nvmf.jvnmf;

public class FabricsConnectResponseCapsule extends ResponseCapsule<FabricsConnectResponseCqe> {

  public FabricsConnectResponseCapsule() {
    super(new FabricsConnectResponseCqe());
  }
}
