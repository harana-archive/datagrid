package com.harana.datagrid.storage.tcp;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.harana.datagrid.storage.StorageFuture;
import com.harana.datagrid.storage.StorageResult;

import com.ibm.narpc.NaRPCFuture;

public class TcpStorageFuture implements StorageFuture, StorageResult {
	private NaRPCFuture<TcpStorageRequest, TcpStorageResponse> future;
	private int len;
	
	public TcpStorageFuture(NaRPCFuture<TcpStorageRequest, TcpStorageResponse> future, int len) {
		this.future = future;
		this.len = len;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public boolean isDone() {
		return future.isDone();
	}

	@Override
	public StorageResult get() throws InterruptedException, ExecutionException {
		future.get();
		return this;
	}

	@Override
	public StorageResult get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		future.get(timeout, unit);
		return this;
	}

	@Override
	public boolean isSynchronous() {
		return false;
	}

	@Override
	public int getLen() {
		return len;
	}

}
