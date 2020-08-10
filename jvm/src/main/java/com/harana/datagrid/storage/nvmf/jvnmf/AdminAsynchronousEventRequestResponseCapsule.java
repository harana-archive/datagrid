package com.harana.datagrid.storage.nvmf.jvnmf;

public class AdminAsynchronousEventRequestResponseCapsule extends
    ResponseCapsule<AdminAsynchronousEventRequestResponseCqe> {

  public AdminAsynchronousEventRequestResponseCapsule() {
    super(new AdminAsynchronousEventRequestResponseCqe());
  }
}
