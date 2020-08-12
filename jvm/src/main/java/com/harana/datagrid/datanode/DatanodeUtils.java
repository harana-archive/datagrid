package com.harana.datagrid.datanode;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.util.List;
import java.util.Objects;

public class DatanodeUtils {
	public static InetSocketAddress getDataNodeAddress(String ifname, int port) throws IOException {
		NetworkInterface netif = NetworkInterface.getByName(ifname);
		if (netif == null) {
			throw new IOException("Cannot find network interface with name " + ifname);
		}
		List<InterfaceAddress> addresses = netif.getInterfaceAddresses();
		InetAddress addr = null;
		for (InterfaceAddress address: addresses) {
			if (address.getBroadcast() != null) {
				addr = address.getAddress();
			}
		}

		if (addr == null) {
			throw new IOException("Network interface with name " + ifname + " has no valid IP address");
		}
		return new InetSocketAddress(addr, port);
	}
	
	public static void clean(String base, String path) throws IOException {
		try {
			File dataDir = new File(path);
			if (!dataDir.exists()) {
				if (!dataDir.mkdirs()) {
					throw new IOException("datapath " + base + " either does not exist or has no write permissions");
				}
			}
			for (File child : Objects.requireNonNull(dataDir.listFiles())) {
				child.delete();
			}
		} catch(SecurityException e) {
			throw new IOException("Error when trying to access " + base, e);
		}
	}
	
	public static String getDatanodeDirectory(String datapath, InetSocketAddress address) throws IllegalArgumentException {
		if (address == null) {
			throw new IllegalArgumentException("Address paramater cannot be null!");
		}
		return datapath + address.getAddress() + "-"  + address.getPort();
	}	
}