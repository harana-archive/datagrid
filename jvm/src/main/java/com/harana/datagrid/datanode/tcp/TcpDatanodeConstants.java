package com.harana.datagrid.datanode.tcp;

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

import java.util.Arrays;

public class TcpDatanodeConstants {
	private static final Logger logger = LogManager.getLogger();
	
	public static final String STORAGE_TCP_INTERFACE_KEY = "crail.storage.tcp.interface";
	public static String STORAGE_TCP_INTERFACE = "eth0";
	
	public static final String STORAGE_TCP_PORT_KEY = "crail.storage.tcp.port";
	public static int STORAGE_TCP_PORT = 50020;
	
	public static final String STORAGE_TCP_STORAGE_LIMIT_KEY = "crail.storage.tcp.storagelimit";
	public static long STORAGE_TCP_STORAGE_LIMIT = 1073741824;

	public static final String STORAGE_TCP_ALLOCATION_SIZE_KEY = "crail.storage.tcp.allocationsize";
	public static long STORAGE_TCP_ALLOCATION_SIZE = Constants.REGION_SIZE;
	
	public static final String STORAGE_TCP_DATA_PATH_KEY = "crail.storage.tcp.datapath";
	public static String STORAGE_TCP_DATA_PATH = "/dev/hugepages/data";
	
	public static final String STORAGE_TCP_QUEUE_DEPTH_KEY = "crail.storage.tcp.queuedepth";
	public static int STORAGE_TCP_QUEUE_DEPTH = 16;	
	
	public static final String STORAGE_TCP_CORES_KEY = "crail.storage.tcp.cores";
	public static int STORAGE_TCP_CORES = 1;		
	
    public static void init(Configuration conf, String[] args) throws Exception {
        if (args != null) {
                Option portOption = Option.builder("p").desc("port to start server on").hasArg().build();
                Option coresOption = Option.builder("c").desc("number of cores to use").hasArg().build();
                Options options = new Options();
                options.addOption(portOption);
                options.addOption(coresOption);
                CommandLineParser parser = new DefaultParser();

                try {
                        CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
                        if (line.hasOption(portOption.getOpt())) {
                                String port = line.getOptionValue(portOption.getOpt());
                                logger.info("using custom port " + port);
                                conf.set(TcpDatanodeConstants.STORAGE_TCP_PORT_KEY, port);
                        }
                        if (line.hasOption(coresOption.getOpt())) {
                            String cores = line.getOptionValue(coresOption.getOpt());
                            logger.info("number of cores used is " + cores);
                            conf.set(TcpDatanodeConstants.STORAGE_TCP_CORES_KEY, cores);
                        }                        
                        
                } catch (ParseException e) {
                        HelpFormatter formatter = new HelpFormatter();
                        formatter.printHelp("RDMA storage tier", options);
                        System.exit(-1);
                }
        }

        TcpDatanodeConstants.updateConstants(conf);
    }
	
	public static void updateConstants(Configuration conf) {
		if (conf.get(STORAGE_TCP_INTERFACE_KEY) != null) {
			STORAGE_TCP_INTERFACE = conf.get(STORAGE_TCP_INTERFACE_KEY);
		}	
		if (conf.get(STORAGE_TCP_PORT_KEY) != null) {
			STORAGE_TCP_PORT = Integer.parseInt(conf.get(STORAGE_TCP_PORT_KEY));
		}		
		if (conf.get(STORAGE_TCP_STORAGE_LIMIT_KEY) != null) {
			STORAGE_TCP_STORAGE_LIMIT = Long.parseLong(conf.get(STORAGE_TCP_STORAGE_LIMIT_KEY));
		}			
		if (conf.get(STORAGE_TCP_ALLOCATION_SIZE_KEY) != null) {
			STORAGE_TCP_ALLOCATION_SIZE = Integer.parseInt(conf.get(STORAGE_TCP_ALLOCATION_SIZE_KEY));
		}			
		if (conf.get(STORAGE_TCP_DATA_PATH_KEY) != null) {
			STORAGE_TCP_DATA_PATH = conf.get(STORAGE_TCP_DATA_PATH_KEY);
		}	
		if (conf.get(STORAGE_TCP_QUEUE_DEPTH_KEY) != null) {
			STORAGE_TCP_QUEUE_DEPTH = Integer.parseInt(conf.get(STORAGE_TCP_QUEUE_DEPTH_KEY));
		}
		if (conf.get(STORAGE_TCP_CORES_KEY) != null) {
			STORAGE_TCP_CORES = Integer.parseInt(conf.get(STORAGE_TCP_CORES_KEY));
		}		
	}	
	
	public static void printConf(Logger logger) {
		logger.info(STORAGE_TCP_INTERFACE_KEY + " " + STORAGE_TCP_INTERFACE);
		logger.info(STORAGE_TCP_PORT_KEY + " " + STORAGE_TCP_PORT);		
		logger.info(STORAGE_TCP_STORAGE_LIMIT_KEY + " " + STORAGE_TCP_STORAGE_LIMIT);
		logger.info(STORAGE_TCP_ALLOCATION_SIZE_KEY + " " + STORAGE_TCP_ALLOCATION_SIZE);
		logger.info(STORAGE_TCP_DATA_PATH_KEY + " " + STORAGE_TCP_DATA_PATH);
		logger.info(STORAGE_TCP_QUEUE_DEPTH_KEY + " " + STORAGE_TCP_QUEUE_DEPTH);
		logger.info(STORAGE_TCP_CORES_KEY + " " + STORAGE_TCP_CORES);
	}	

}
