package com.harana.datagrid.namenode.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.DelayQueue;

public class GCServer implements Runnable {
	private static final Logger logger = LogManager.getLogger();
	
	private NameNodeService rpcService;
	private DelayQueue<AbstractNode> deleteQueue;
	
	public GCServer(NameNodeService service, DelayQueue<AbstractNode> deleteQueue) {
		this.rpcService = service;
		this.deleteQueue = deleteQueue;
	}

	@Override
	public void run() {
		while (true) {
			try{
				AbstractNode file = deleteQueue.take();
				if (file.getType().isContainer()) {
					file.clearChildren(deleteQueue);
				}
				rpcService.freeFile(file);
			} catch(Exception e) {
				logger.info("Exception during GC: " + e.getMessage());
			}
		}
	}
}