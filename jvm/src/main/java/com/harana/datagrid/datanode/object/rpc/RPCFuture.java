package com.harana.datagrid.datanode.object.rpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RPCFuture<T> extends ObjectStoreCommonFuture implements Future<T> {
	private final T result;
	private final String debug;
	private boolean prefetch;
	private boolean done;

	public RPCFuture(String name, T result) {
		this.debug = name;
		this.result = result;
		this.done = false;
	}

	public void markDone() {
		synchronized (this) {
			this.done = true;
			this.notifyAll();
		}
	}

	public int getTicket() {
		return this.hashCode();
	}

	public boolean isPrefetched() {
		return this.prefetch;
	}

	public void setPrefetched(boolean b) {
		this.prefetch = b;
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}

	public boolean isDone() {
		return this.done;
	}

	public T get() throws InterruptedException, ExecutionException {
		synchronized (this) {
			if (!isDone()) {
				this.wait();
			}
		}
		if (!isDone())
			throw new InterruptedException("RPC interrupted: " + debug);

		return result;
	}

	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		synchronized (this) {
			if (!isDone()) {
				this.wait(unit.toMillis(timeout));
			}
		}
		if (!isDone())
			throw new TimeoutException("RPC timeout: " + debug);

		return result;
	}

	public T getResult() {
		return result;
	}
}
