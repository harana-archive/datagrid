package com.harana.datagrid.rpc.rdma;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.harana.datagrid.rpc.RpcFuture;
import com.harana.datagrid.darpc.DaRPCFuture;

public class RdmaNameNodeFuture<T> implements RpcFuture<T> {
	private DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future;
	private boolean prefetched;
	private T response;
	
	public RdmaNameNodeFuture(DaRPCFuture<RdmaNameNodeRequest, RdmaNameNodeResponse> future, T response) {
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