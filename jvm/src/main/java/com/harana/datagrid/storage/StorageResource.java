package com.harana.datagrid.storage;

public class StorageResource {
	private long address;
	private int length;
	private int key;
	
	public static StorageResource createResource(long address, int length, int key){
		return new StorageResource(address, length, key);
	}
	
	private StorageResource(long address, int length, int key){
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
