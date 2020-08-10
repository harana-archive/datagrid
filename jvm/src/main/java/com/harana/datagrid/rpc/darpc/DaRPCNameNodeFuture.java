package com.harana.datagrid.rpc.darpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.harana.datagrid.rpc.RpcFuture;

import com.harana.datagrid.darpc.DaRPCFuture;

public class DaRPCNameNodeFuture<T> implements RpcFuture<T> {
	private DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future;
	private boolean prefetched;
	private T response;
	
	public DaRPCNameNodeFuture(DaRPCFuture<DaRPCNameNodeRequest, DaRPCNameNodeResponse> future, T response) {
		this.future = future;
		this.response = response;
		this.prefetched = false;
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		future.get();
		return response;
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		future.get(timeout, unit);
		return response;
	}
	
	@Override
	public boolean isDone() {
		return future.isDone();
	}	

	@Override
	public int getTicket() {
		return future.getTicket();
	}
	
	@Override
	public boolean isPrefetched() {
		return prefetched;
	}

	@Override
	public void setPrefetched(boolean prefetched) {
		this.prefetched = prefetched;
	}	

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return future.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return future.isCancelled();
	}
}