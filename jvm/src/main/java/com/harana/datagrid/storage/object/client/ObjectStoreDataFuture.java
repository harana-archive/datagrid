package com.harana.datagrid.storage.object.client;

import com.harana.datagrid.storage.object.ObjectStoreUtils;
import com.harana.datagrid.storage.StorageFuture;
import com.harana.datagrid.storage.StorageResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectStoreDataFuture implements StorageFuture, StorageResult {

	private static final Logger logger = LogManager.getLogger();
	private static final AtomicInteger hash = new AtomicInteger(0);
	private final int hashCode;
	private final int length;

	public ObjectStoreDataFuture(int length) {
		this.hashCode = hash.getAndIncrement();
		this.length = length;
	}

	public int getLen() {
		return this.length;
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}

	public boolean isDone() {
		return true;
	}

	public final StorageResult get() {
		return this;
	}

	public StorageResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return this;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object o) {
		return hashCode == o.hashCode();
	}

	@Override
	public boolean isSynchronous() {
		return true;
	}
}
