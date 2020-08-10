package com.harana.datagrid.datanode.rdma.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.harana.datagrid.client.datanode.DatanodeFuture;
import com.harana.datagrid.client.datanode.DatanodeResult;

public class RdmaActiveFuture implements DatanodeFuture, DatanodeResult {
	protected static int RPC_PENDING = 0;
	protected static int RPC_DONE = 1;
	protected static int RPC_ERROR = 2;		
	
	private long wrid;
	private int len;
	private boolean isWrite;
	private AtomicInteger status;

	public RdmaActiveFuture(long wrid, int len, boolean isWrite) {
		this.wrid = wrid;
		this.len = len;
		this.isWrite = isWrite;	
		this.status = new AtomicInteger(RPC_PENDING);
	}	
	
	public long getWrid() {
		return wrid;
	}
	
	@Override
	public synchronized DatanodeResult get() throws InterruptedException, ExecutionException {
		if (status.get() == RPC_PENDING) {
			try {
				wait();
			} catch (Exception e) {
				status.set(RPC_ERROR);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		if (status.get() == RPC_DONE) {
			return this;
		} else if (status.get() == RPC_PENDING) {
			throw new InterruptedException("RPC timeout");
		} else {
			throw new InterruptedException("RPC error");
		}
	}

	@Override
	public synchronized DatanodeResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
		if (status.get() == RPC_PENDING) {
			try {
				wait(timeout);
			} catch (Exception e) {
				status.set(RPC_ERROR);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		if (status.get() == RPC_DONE) {
			return this;
		} else if (status.get() == RPC_PENDING) {
			throw new InterruptedException("RPC timeout");
		} else {
			throw new InterruptedException("RPC error");
		}
	}	
	
	public boolean isDone() {
		return status.get() > 0;
	}
	
	public synchronized void signal() {
		status.set(RPC_DONE);
		notify();
	}

	public int getLen() {
		if (status.get() == RPC_DONE) {
			return len;
		} else if (status.get() == RPC_PENDING) {
			return 0;
		} else {
			return -1;
		}
	}

	public boolean isWrite() {
		return isWrite;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCancelled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isSynchronous() {
		return false;
	}
}
