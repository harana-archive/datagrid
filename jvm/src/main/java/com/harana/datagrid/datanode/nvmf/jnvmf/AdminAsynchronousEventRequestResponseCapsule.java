package com.harana.datagrid.datanode.nvmf.jnvmf;

public class AdminAsynchronousEventRequestResponseCapsule extends
    ResponseCapsule<AdminAsynchronousEventRequestResponseCqe> {

  public AdminAsynchronousEventRequestResponseCapsule() {
    super(new AdminAsynchronousEventRequestResponseCqe());
  }
}
