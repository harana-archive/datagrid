package com.harana.datagrid.datanode.nvmf.jvnmf;

public class AdminAsynchronousEventRequestResponseCapsule extends
    ResponseCapsule<AdminAsynchronousEventRequestResponseCqe> {

  public AdminAsynchronousEventRequestResponseCapsule() {
    super(new AdminAsynchronousEventRequestResponseCqe());
  }
}
