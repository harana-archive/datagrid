package com.harana.datagrid.namenode;

import java.net.URI;
import java.util.Arrays;

import com.harana.datagrid.namenode.storage.LogDispatcher;
import com.harana.datagrid.utils.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.harana.datagrid.conf.Configuration;
import com.harana.datagrid.conf.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NameNode {
	private static final Logger logger = LogManager.getLogger();
	
	public static void main(String args[]) throws Exception {
		logger.info("initalizing namenode ");		
		Configuration conf = Configuration.createConfigurationFromFile();
		Constants.updateConstants(conf);
		
		URI uri = Utils.getPrimaryNameNode();
		String address = uri.getHost();
		int port = uri.getPort();
		
		if (args != null) {
			Option addressOption = Option.builder("a").desc("ip address namenode is started on").hasArg().build();
			Option portOption = Option.builder("p").desc("port namenode is started on").hasArg().build();
			Options options = new Options();
			options.addOption(portOption);
			options.addOption(addressOption);
			CommandLineParser parser = new DefaultParser();
			
			try {
				CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
				if (line.hasOption(addressOption.getOpt())) {
					address = line.getOptionValue(addressOption.getOpt());
				}					
				if (line.hasOption(portOption.getOpt())) {
					port = Integer.parseInt(line.getOptionValue(portOption.getOpt()));
				}				
			} catch (ParseException e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp("Namenode", options);
				System.exit(-1);
			}
		}		
		
		String namenode = "crail://" + address + ":" + port;
		long serviceId = Utils.getServiceId(namenode);
		long serviceSize = Utils.getServiceSize();
		if (!Utils.verifyNamenode(namenode)) {
			throw new Exception("Namenode address/port [" + namenode + "] has to be listed in crail.namenode.address " + Constants.NAMENODE_ADDRESS);
		}
		
		Constants.NAMENODE_ADDRESS = namenode + "?id=" + serviceId + "&size=" + serviceSize;
		Constants.printConf();
		Constants.verify();
		
		RpcNameNodeService service = RpcNameNodeService.createInstance(Constants.NAMENODE_RPC_SERVICE);

		// TODO: Configurable logging
		service = new LogDispatcher(service);

		RpcBinding rpcBinding = RpcBinding.createInstance(Constants.NAMENODE_RPC_TYPE);
		NamenodeServer rpcServer = rpcBinding.launchServer(service);
		rpcServer.init(conf, null);
		rpcServer.printConf(logger);
		rpcServer.run();
		System.exit(0);;
	}
}