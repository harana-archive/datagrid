package com.harana.datagrid.namenode;

import java.net.URI;
import java.util.Arrays;

import com.harana.datagrid.utils.CrailUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import com.harana.datagrid.conf.CrailConfiguration;
import com.harana.datagrid.conf.CrailConstants;
import com.harana.datagrid.rpc.RpcBinding;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NameNode {
	private static final Logger logger = LogManager.getLogger();
	
	public static void main(String args[]) throws Exception {
		logger.info("initalizing namenode ");		
		CrailConfiguration conf = CrailConfiguration.createConfigurationFromFile();
		CrailConstants.updateConstants(conf);
		
		URI uri = CrailUtils.getPrimaryNameNode();
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
		long serviceId = CrailUtils.getServiceId(namenode);
		long serviceSize = CrailUtils.getServiceSize();
		if (!CrailUtils.verifyNamenode(namenode)){
			throw new Exception("Namenode address/port [" + namenode + "] has to be listed in crail.namenode.address " + CrailConstants.NAMENODE_ADDRESS);
		}
		
		CrailConstants.NAMENODE_ADDRESS = namenode + "?id=" + serviceId + "&size=" + serviceSize;
		CrailConstants.printConf();
		CrailConstants.verify();
		
		RpcNameNodeService service = RpcNameNodeService.createInstance(CrailConstants.NAMENODE_RPC_SERVICE);

		// TODO: Configurable logging
		service = new LogDispatcher(service);

		RpcBinding rpcBinding = RpcBinding.createInstance(CrailConstants.NAMENODE_RPC_TYPE);
		RpcServer rpcServer = rpcBinding.launchServer(service);
		rpcServer.init(conf, null);
		rpcServer.printConf(logger);
		rpcServer.run();
		System.exit(0);;
	}
}