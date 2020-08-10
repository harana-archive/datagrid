package com.harana.datagrid.datanode;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.harana.datagrid.client.namenode.NamenodeClient;
import com.harana.datagrid.client.namenode.NamenodeConnection;
import com.harana.datagrid.client.namenode.NamenodeDispatcher;
import com.harana.datagrid.utils.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.harana.datagrid.StorageClass;
import com.harana.datagrid.conf.Configurable;
import com.harana.datagrid.conf.Configuration;
import com.harana.datagrid.conf.Constants;
import com.harana.datagrid.metadata.DatanodeStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public interface DatanodeServer extends Configurable, Runnable {
	DatanodeResource allocateResource() throws Exception;
	boolean isAlive();
	InetSocketAddress getAddress();
	
	static void main(String[] args) throws Exception {
		Logger logger = LogManager.getLogger();
		Configuration conf = Configuration.createConfigurationFromFile();
		Constants.updateConstants(conf);
		Constants.printConf();
		Constants.verify();
		
		int splitIndex = 0;
		for (String param : args) {
			if (param.equalsIgnoreCase("--")) {
				break;
			} 
			splitIndex++;
		}
		
		//default values
		StringTokenizer tokenizer = new StringTokenizer(Constants.STORAGE_TYPES, ",");
		if (!tokenizer.hasMoreTokens()) {
			throw new Exception("No storage types defined!");
		}
		String storageName = tokenizer.nextToken();
		int storageType = 0;
		HashMap<String, Integer> storageTypes = new HashMap<>();
		storageTypes.put(storageName, storageType);
		for (int type = 1; tokenizer.hasMoreElements(); type++) {
			String name = tokenizer.nextToken();
			storageTypes.put(name, type);
		}
		int storageClass = -1;
		
		//custom values
		if (args != null) {
			Option typeOption = Option.builder("t").desc("storage type to start").hasArg().build();
			Option classOption = Option.builder("c").desc("storage class the server will attach to").hasArg().build();
			Options options = new Options();
			options.addOption(typeOption);
			options.addOption(classOption);
			CommandLineParser parser = new DefaultParser();
			
			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, splitIndex));
				if (line.hasOption(typeOption.getOpt())) {
					storageName = line.getOptionValue(typeOption.getOpt());
					storageType = storageTypes.get(storageName);
				}				
				if (line.hasOption(classOption.getOpt())) {
					storageClass = Integer.parseInt(line.getOptionValue(classOption.getOpt()));
				}					
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("Storage tier", options);
				System.exit(-1);
			}
		}
		if (storageClass < 0) {
			storageClass = storageType;
		}
		
		DatanodeTier storageTier = DatanodeTier.createInstance(storageName);
		if (storageTier == null) {
			throw new Exception("Cannot instantiate datanode of type " + storageName);
		}
		DatanodeServer server = storageTier.launchServer();
		
		String[] extraParams = null;
		splitIndex++;
		if (args.length > splitIndex) {
			extraParams = new String[args.length - splitIndex];
			for (int i = splitIndex; i < args.length; i++) {
				extraParams[i-splitIndex] = args[i];
			}
		}
		server.init(conf, extraParams);
		server.printConf(logger);
		
		Thread thread = new Thread(server);
		thread.start();
		
		NamenodeClient namenodeClient = NamenodeClient.createInstance(Constants.NAMENODE_RPC_TYPE);
		namenodeClient.init(conf, args);
		namenodeClient.printConf(logger);
		
		ConcurrentLinkedQueue<InetSocketAddress> namenodeList = Utils.getNameNodeList();
		ConcurrentLinkedQueue<NamenodeConnection> connectionList = new ConcurrentLinkedQueue<>();
		while (!namenodeList.isEmpty()) {
			InetSocketAddress address = namenodeList.poll();
			NamenodeConnection connection = namenodeClient.connect(address);
			connectionList.add(connection);
		}
		NamenodeConnection NamenodeConnection = connectionList.peek();
		if (connectionList.size() > 1) {
			NamenodeConnection = new NamenodeDispatcher(connectionList);
		}
		logger.info("connected to namenode(s) " + NamenodeConnection.toString());
		
		
		DatanodeStorage datanodeStorage = new DatanodeStorage(storageType, StorageClass.get(storageClass), server.getAddress(), NamenodeConnection);
		
		HashMap<Long, Long> blockCount = new HashMap<>();
		long sumCount = 0;
		long lba = 0;
		while (server.isAlive()) {
			DatanodeResource resource = server.allocateResource();
			if (resource == null) {
				break;
			} else {
				datanodeStorage.setBlock(lba, resource.getAddress(), resource.getLength(), resource.getKey());
				lba += resource.getLength();
				
				DatanodeStatistics stats = datanodeStorage.getDataNode();
				long newCount = stats.getFreeBlockCount();
				long serviceId = stats.getServiceId();
				
				long oldCount = 0;
				if (blockCount.containsKey(serviceId)) {
					oldCount = blockCount.get(serviceId);
				}
				long diffCount = newCount - oldCount;
				blockCount.put(serviceId, newCount);
				sumCount += diffCount;
				logger.info("datanode statistics, freeBlocks " + sumCount);		
			}
		}
		
		while (server.isAlive()) {
			DatanodeStatistics stats = datanodeStorage.getDataNode();
			long newCount = stats.getFreeBlockCount();
			long serviceId = stats.getServiceId();
			
			long oldCount = 0;
			if (blockCount.containsKey(serviceId)) {
				oldCount = blockCount.get(serviceId);
			}
			long diffCount = newCount - oldCount;
			blockCount.put(serviceId, newCount);
			sumCount += diffCount;			
			
			logger.info("datanode statistics, freeBlocks " + sumCount);
			Thread.sleep(Constants.STORAGE_KEEPALIVE*1000);
		}			
	}
}
