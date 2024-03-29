package com.harana.datagrid.datanode.object;

import com.harana.datagrid.conf.DatagridConfiguration;
import com.harana.datagrid.conf.DatagridConstants;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;

public class ObjectStoreConstants {
	static private final Logger logger = LogManager.getLogger();

	private final static String PREFIX = "storage.object";

	/* General object storage options */
	private static final String STORAGE_LIMIT_KEY = "storagelimit";
	public static long STORAGE_LIMIT = 1073741824; /* 1GB */
	private static final String ALLOCATION_SIZE_KEY = "allocationsize";
	public static long ALLOCATION_SIZE = 1048576; /* 1MB */
	private static final String DATANODE_KEY = "datanode";
	public static String DATANODE = "localhost";
	private static final String DATANODE_PORT_KEY = "datanodeport";
	public static int DATANODE_PORT = 50080;
	private static final String OBJECT_PREFIX_KEY = "objectprefix";
	public static String OBJECT_PREFIX = "crail";
	private static final String CLEANUP_ON_EXIT_KEY = "cleanuponexit";
	public static Boolean CLEANUP_ON_EXIT = false;
	private static final String PROFILE_KEY = "profile";
	public static Boolean PROFILE = false;

	/* S3 specific connection options */
	private static final String S3_ACCESS_KEY = "s3accesskey";
	public static String S3_ACCESS = null;
	private static final String S3_SECRET_KEY = "s3secretkey";
	public static String S3_SECRET = null;
	private static final String S3_REGION_NAME_KEY = "s3region";
	public static String S3_REGION_NAME = null;
	private static final String S3_BUCKET_NAME_KEY = "s3bucket";
	public static String S3_BUCKET_NAME = "crail";
	private static final String S3_ENDPOINT_KEY = "s3endpoint";
	public static String S3_ENDPOINT = null;
	private static final String S3_PROTOCOL_KEY = "s3protocol";
	public static String S3_PROTOCOL = "HTTP";
	private static final String S3_SIGNER_KEY = "s3signer";
	public static String S3_SIGNER = null; //other possible values "AWS3SignerType", "AWS4SignerType", "NoOpSignerType"

	public static void parseCmdLine(DatagridConfiguration conf, String[] args) throws IOException {
		ObjectStoreConstants.updateConstants(conf);
		if (args != null) {
			Options options = new Options();

			Option env = Option.builder("e").
					longOpt("use-env").
					desc("Use environment variables to set default parameter values").
					hasArg().
					build();
			options.addOption(env);
			CommandLineParser parser = new DefaultParser();
			HelpFormatter formatter = new HelpFormatter();
			CommandLine line = null;
			try {
				line = parser.parse(options, args);
			} catch (ParseException e) {
				logger.error("Invalid command line option", e);
				formatter.printHelp("Object storage tier", options);
				System.exit(-1);
			}
			if (line.hasOption(env.getOpt())) {
				importS3EnvConf();
			}
		}
		ObjectStoreConstants.verify();
	}

	public static void updateConstants(DatagridConfiguration conf) {
		String arg = get(conf, S3_ACCESS_KEY);
		if (arg != null) S3_ACCESS = arg;

		arg = get(conf, S3_SECRET_KEY);
		if (arg != null) S3_SECRET = arg;

		arg = get(conf, S3_REGION_NAME_KEY);
		if (arg != null) S3_REGION_NAME = arg;

		arg = get(conf, S3_BUCKET_NAME_KEY);
		if (arg != null) S3_BUCKET_NAME = arg;

		arg = get(conf, S3_ENDPOINT_KEY);
		if (arg != null) S3_ENDPOINT = arg;

		arg = get(conf, S3_PROTOCOL_KEY);
		if (arg != null) S3_PROTOCOL = arg.toUpperCase();

		arg = get(conf, S3_SIGNER_KEY);
		if (arg != null) S3_SIGNER = arg;

		arg = get(conf, OBJECT_PREFIX_KEY);
		if (arg != null) OBJECT_PREFIX = arg;

		arg = get(conf, CLEANUP_ON_EXIT_KEY);
		if (arg != null) CLEANUP_ON_EXIT = Boolean.valueOf(arg);

		arg = get(conf, STORAGE_LIMIT_KEY);
		if (arg != null) STORAGE_LIMIT = Long.parseLong(arg);

		arg = get(conf, ALLOCATION_SIZE_KEY);
		if (arg != null) ALLOCATION_SIZE = Long.parseLong(arg);

		arg = get(conf, DATANODE_KEY);
		if (arg != null) DATANODE = arg;

		arg = get(conf, DATANODE_PORT_KEY);
		if (arg != null) DATANODE_PORT = Integer.parseInt(arg);

		arg = get(conf, PROFILE_KEY);
		if (arg != null) PROFILE = Boolean.valueOf(arg);
	}

	public static void verify() throws IOException {
		if (ALLOCATION_SIZE % DatagridConstants.BLOCK_SIZE != 0) {
			logger.error("Allocation size ({}) must be a multiple of the crail blocksize ({})", ALLOCATION_SIZE, DatagridConstants.BLOCK_SIZE);
			throw new IOException("Allocation size must be multiple of the  blocksize");
		}
		if (STORAGE_LIMIT % ALLOCATION_SIZE != 0) {
			logger.error("Storage limit ({}) must be multiple of the allocation size ({})", STORAGE_LIMIT, ALLOCATION_SIZE);
			throw new IOException("Storage limit must be multiple of the allocation size");
		}
	}

	private static String get(DatagridConfiguration conf, String key) {
		return conf.get(fullKey(key));
	}

	private static String fullKey(String key) {
		return PREFIX + "." + key;
	}

	public static void printConf(Logger logger) {
		Field[] fields = ObjectStoreConstants.class.getDeclaredFields();
		for (Field field : fields) {
			if (field.getName().endsWith("_KEY")) {
				String fieldValueName = field.getName().replaceFirst("_KEY$", "");
				try {
					Field value = ObjectStoreConstants.class.getField(fieldValueName);
					logger.info(fullKey(field.get(null).toString()) + " = " + value.get(null));
				} catch (Exception e) {
					logger.warn("Cannot find matching field for {}", field.getName());
				}
			}
		}
	}

	private static void importS3EnvConf() {
		String envVal;
		envVal = System.getenv("S3_ACCESS_KEY");
		if (envVal != null) {
			logger.debug("Setting {} from {} to {} environemnt value", fullKey(ObjectStoreConstants.S3_ACCESS_KEY), ObjectStoreConstants.S3_ACCESS, envVal);
			ObjectStoreConstants.S3_ACCESS = envVal;
		}
		envVal = System.getenv("S3_SECRET_KEY");
		if (envVal != null) {
			logger.debug("Setting {} from {} to {} environemnt value", fullKey(ObjectStoreConstants.S3_SECRET_KEY), ObjectStoreConstants.S3_SECRET, envVal);
			ObjectStoreConstants.S3_SECRET = envVal;
		}
		envVal = System.getenv("S3_REGION_NAME");
		if (envVal != null) {
			ObjectStoreConstants.S3_REGION_NAME = envVal;
		}
		envVal = System.getenv("S3_BUCKET_NAME");
		if (envVal != null) {
			logger.debug("Setting {} from {} to {} environemnt value", fullKey(ObjectStoreConstants.S3_BUCKET_NAME_KEY), ObjectStoreConstants.S3_BUCKET_NAME, envVal);
			ObjectStoreConstants.S3_BUCKET_NAME = envVal;
		}
		envVal = System.getenv("S3_ENDPOINT");
		if (envVal != null) {
			logger.debug("Setting {} from {} to {} environemnt value", fullKey(ObjectStoreConstants.S3_ENDPOINT_KEY), ObjectStoreConstants.S3_ENDPOINT, envVal);
			ObjectStoreConstants.S3_ENDPOINT = envVal;
		}
		envVal = System.getenv("S3_PROTOCOL");
		if (envVal != null) {
			logger.debug("Setting {} from {} to {} environemnt value", fullKey(ObjectStoreConstants.S3_PROTOCOL_KEY), ObjectStoreConstants.S3_PROTOCOL, envVal);
			ObjectStoreConstants.S3_PROTOCOL = envVal;
		}
		envVal = System.getenv("S3_SIGNER");
		if (envVal != null) {
			logger.debug("Setting {} from {} to {} environemnt value", fullKey(ObjectStoreConstants.S3_SECRET_KEY), ObjectStoreConstants.S3_SIGNER, envVal);
			ObjectStoreConstants.S3_SIGNER = envVal;
		}
	}
}
