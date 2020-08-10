package com.harana.datagrid.storage.nvmf.jvnmf;

public class FabricsConnectResponseCapsule extends ResponseCapsule<FabricsConnectResponseCqe> {

  public FabricsConnectResponseCapsule() {
    super(new FabricsConnectResponseCqe());
  }
}
