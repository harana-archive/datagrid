package com.harana.datagrid.storage.nvmf.jvnmf;

public class FabricsPropertyGetResponseCapsule extends
    ResponseCapsule<FabricsPropertyGetResponseCqe> {

  public FabricsPropertyGetResponseCapsule() {
    super(new FabricsPropertyGetResponseCqe());
  }
}
