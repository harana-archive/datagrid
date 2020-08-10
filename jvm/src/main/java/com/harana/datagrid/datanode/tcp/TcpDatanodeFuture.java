package com.harana.datagrid.datanode.tcp;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.harana.datagrid.client.datanode.DatanodeFuture;
import com.harana.datagrid.client.datanode.DatanodeResult;
import com.harana.datagrid.rpc.narpc.NaRPCFuture;

public class TcpDatanodeFuture implements DatanodeFuture, DatanodeResult {
	private final NaRPCFuture<TcpDatanodeRequest, TcpDatanodeResponse> future;
	private final int len;
	
	public TcpDatanodeFuture(NaRPCFuture<TcpDatanodeRequest, TcpDatanodeResponse> future, int len) {
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
	public DatanodeResult get() throws InterruptedException, ExecutionException {
		future.get();
		return this;
	}

	@Override
	public DatanodeResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
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