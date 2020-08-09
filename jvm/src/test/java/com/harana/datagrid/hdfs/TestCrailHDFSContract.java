package com.harana.datagrid.hdfs;

import java.net.URI;

import com.harana.datagrid.hdfs.CrailHadoopFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;

public class TestCrailHDFSContract extends FileSystemContractBaseTest {

	@Override
	protected void setUp() throws Exception {
		Configuration conf = new Configuration();
		fs = new CrailHadoopFileSystem();
		fs.initialize(URI.create(conf.get("fs.defaultFS")), conf);
	}

	// --------------------

	protected final static String TEST_UMASK = "062";
	protected FileSystem fs;
	protected byte[] data = dataset(getBlockSize() * 2, 0, 255);

	@Override
	protected void tearDown() throws Exception {
		fs.delete(path("/test"), true);
		fs.close();
	}

	/**
	 * Create a dataset for use in the tests; all data is in the range base to
	 * (base+modulo-1) inclusive
	 * 
	 * @param len
	 *            length of data
	 * @param base
	 *            base of the data
	 * @param modulo
	 *            the modulo
	 * @return the newly generated dataset
	 */
	protected byte[] dataset(int len, int base, int modulo) {
		byte[] dataset = new byte[len];
		for (int i = 0; i < len; i++) {
			dataset[i] = (byte) (base + (i % modulo));
		}
		return dataset;
	}

}
