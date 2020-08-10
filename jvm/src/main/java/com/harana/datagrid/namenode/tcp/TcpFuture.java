package com.harana.datagrid.namenode.tcp;

import com.harana.datagrid.client.namenode.NamenodeFuture;
import com.harana.datagrid.rpc.narpc.NaRPCFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TcpFuture<T> implements NamenodeFuture<T> {
	private final NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future;
	private final T response;
	private boolean prefetched;
	
	public TcpFuture(NaRPCFuture<TcpNamenodeRequest, TcpNamenodeResponse> future, T resp) {
		this.future = future;
		this.response = resp;
		this.prefetched = false;
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
	public T get() throws InterruptedException, ExecutionException {
		future.get();
		return response;
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		future.get(timeout, unit);
		return response;
	}

	@Override
	public int getTicket() {
		return (int) future.getTicket();
	}

	@Override
	public boolean isPrefetched() {
		return prefetched;
	}

	@Override
	public void setPrefetched(boolean prefetched) {
		this.prefetched = prefetched;
	}
}