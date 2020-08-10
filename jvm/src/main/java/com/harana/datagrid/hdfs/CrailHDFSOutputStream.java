package com.harana.datagrid.hdfs;

import com.harana.datagrid.CrailBufferedOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;

public class CrailHDFSOutputStream extends FSDataOutputStream {
	public CrailHDFSOutputStream(CrailBufferedOutputStream outputStream, Statistics stats) {
		super(outputStream, stats);
	}
}