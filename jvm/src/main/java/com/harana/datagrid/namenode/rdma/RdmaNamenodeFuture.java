package com.harana.datagrid.namenode.rdma;

import com.harana.datagrid.client.namenode.NamenodeFuture;
import com.harana.datagrid.rpc.darpc.DaRPCFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RdmaNamenodeFuture<T> implements NamenodeFuture<T> {
	private final DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future;
	private final T response;

	private boolean prefetched;

	public RdmaNamenodeFuture(DaRPCFuture<RdmaNamenodeRequest, RdmaNamenodeResponse> future, T response) {
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