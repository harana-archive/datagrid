package com.harana.datagrid.storage.nvmf.jvnmf;

public class AsynchronousEventInformationVendorSpecific extends AsynchronousEventInformation {
  private static final AsynchronousEventInformationVendorSpecific instance =
      new AsynchronousEventInformationVendorSpecific();

  public static AsynchronousEventInformationVendorSpecific getInstance() {
    return instance;
  }
}
