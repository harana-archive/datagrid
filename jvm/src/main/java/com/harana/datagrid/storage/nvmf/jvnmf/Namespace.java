package com.harana.datagrid.storage.nvmf.jvnmf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Namespace {

  // TODO: A namespace can have multiple controllers for fault tolerance etc
  private final Controller controller;
  private final NamespaceIdentifier namespaceIdentifier;
  private final IdentifyNamespaceData identifyNamespaceData;

  private final AdminIdentifyNamespaceCommand command;

  Namespace(Controller controller, NamespaceIdentifier namespaceIdentifier) throws IOException {
    this.controller = controller;
    this.namespaceIdentifier = namespaceIdentifier;
    ByteBuffer buffer = ByteBuffer.allocateDirect(IdentifyNamespaceData.SIZE);
    KeyedNativeBuffer registeredBuffer = controller.getAdminQueue().registerMemory(buffer);
    this.identifyNamespaceData = new IdentifyNamespaceData(registeredBuffer);
    this.command = new AdminIdentifyNamespaceCommand(controller.getAdminQueue());
    AdminIdentifyNamespaceCommandCapsule commandCapsule = command.getCommandCapsule();
    commandCapsule.setSglDescriptor(identifyNamespaceData);
    AdminIdentifyCommandSqe sqe = commandCapsule.getSubmissionQueueEntry();
    sqe.setNamespaceIdentifier(namespaceIdentifier);
  }

  public boolean isActive() {
    //TODO
    return false;
  }

  public NamespaceIdentifier getIdentifier() {
    return namespaceIdentifier;
  }

  public Controller getController() {
    return controller;
  }

  private void updateIdentifyNamespaceData() throws IOException {
    Future<?> commandFuture = command.newCommandFuture();
    ResponseFuture<AdminResponseCapsule> responseFuture = command.newResponseFuture();
    command.execute(responseFuture);
    try {
      commandFuture.get();
      responseFuture.get();
    } catch (InterruptedException exception) {
      throw new IOException(exception);
    } catch (ExecutionException exception) {
      throw new IOException(exception);
    }
  }

  public IdentifyNamespaceData getIdentifyNamespaceData() throws IOException {
    updateIdentifyNamespaceData();
    return identifyNamespaceData;
  }
}
