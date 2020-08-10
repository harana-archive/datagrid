package com.harana.datagrid.datanode;

public class DatanodeResource {
	private final long address;
	private final int length;
	private final int key;
	
	public static DatanodeResource createResource(long address, int length, int key) {
		return new DatanodeResource(address, length, key);
	}
	
	private DatanodeResource(long address, int length, int key) {
		this.address = address;
		this.length = length;
		this.key = key;
	}

	public long getAddress() {
		return address;
	}

	public int getLength() {
		return length;
	}

	public int getKey() {
		return key;
	}
}
