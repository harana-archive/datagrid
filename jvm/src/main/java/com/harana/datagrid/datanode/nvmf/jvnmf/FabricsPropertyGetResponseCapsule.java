package com.harana.datagrid.datanode.nvmf.jvnmf;

public class FabricsPropertyGetResponseCapsule extends
    ResponseCapsule<FabricsPropertyGetResponseCqe> {

  public FabricsPropertyGetResponseCapsule() {
    super(new FabricsPropertyGetResponseCqe());
  }
}
