package com.harana.datagrid.storage.nvmf.jvnmf;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class OperationFuture<O extends Operation, T> implements OperationCallback,
    Future<T> {

  private final QueuePair queuePair;
  private RdmaException rdmaException;
  private final O operation;
  private volatile boolean done;

  OperationFuture(QueuePair queuePair, O operation) {
    this.done = false;
    this.queuePair = queuePair;
    this.operation = operation;
    operation.setCallback(this);
  }

  private final void checkCompleted() {
    if (isDone()) {
      throw new IllegalStateException("Operation already completed");
    }
  }

  @Override
  public void onStart() {
    /* for now we don't allow reusing futures */
  }

  @Override
  public void onComplete() {
    checkCompleted();
    this.done = true;
  }

  @Override
  public void onFailure(RdmaException exception) {
    checkCompleted();
    this.rdmaException = exception;
    this.done = true;
  }

  abstract T getT();

  O getOperation() {
    return operation;
  }

  @Override
  public boolean cancel(boolean cancel) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return done;
  }

  private final void checkStatus() throws ExecutionException {
    if (rdmaException != null) {
      throw new ExecutionException(rdmaException);
    }
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    while (!done) {
      try {
        queuePair.poll();
      } catch (IOException exception) {
        throw new ExecutionException(exception);
      }
    }
    checkStatus();
    return getT();
  }

  @Override
  public T get(long timeout, TimeUnit timeUnit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (!done) {
      long start = System.nanoTime();
      long end = start + TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
      boolean waitTimeOut;
      do {
        try {
          queuePair.poll();
        } catch (IOException exception) {
          throw new ExecutionException(exception);
        }
        waitTimeOut = System.nanoTime() > end;
      } while (!done && !waitTimeOut);
      if (!done && waitTimeOut) {
        throw new TimeoutException("get wait time out!");
      }
    }
    checkStatus();
    return getT();
  }
}
