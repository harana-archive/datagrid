package com.harana.datagrid.datanode.rdma.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.harana.datagrid.client.datanode.DatanodeFuture;
import com.harana.datagrid.client.datanode.DatanodeResult;

import sun.misc.Unsafe;

public class RdmaLocalFuture implements DatanodeFuture, DatanodeResult {
	private Unsafe unsafe;
	private long srcAddr;
	private long dstAddr;
	private int remaining;
	
	private int len;
	private boolean isDone;

	public RdmaLocalFuture(Unsafe unsafe, long srcAddr, long dstAddr, int remaining) {
		this.unsafe = unsafe;
		this.srcAddr = srcAddr;
		this.dstAddr = dstAddr;
		this.remaining = remaining;
		
		this.len = 0;
		this.isDone = false;
	}


	@Override
	public int getLen() {
		return len;
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
		if (!isDone) {
			getDone();
		}		
		return isDone;
	}

	@Override
	public DatanodeResult get() throws InterruptedException, ExecutionException {
		if (!isDone) {
			getDone();
		}
		return this;
	}

	@Override
	public DatanodeResult get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		if (!isDone) {
			getDone();
		}		
		return this;
	}
	
	@Override
	public boolean isSynchronous() {
		return true;
	}
	
	void getDone() {
		unsafe.copyMemory(srcAddr, dstAddr, remaining);
		len = remaining;
		isDone = true;		
	}
}
