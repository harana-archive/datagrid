package com.harana.datagrid.datanode.nvmf.jnvmf;

public class FabricsPropertyGetResponseCapsule extends
    ResponseCapsule<FabricsPropertyGetResponseCqe> {

  public FabricsPropertyGetResponseCapsule() {
    super(new FabricsPropertyGetResponseCqe());
  }
}
