package com.harana.datagrid.hdfs;

import com.harana.datagrid.core.streams.DatagridBufferedOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;

public class HDFSOutputStream extends FSDataOutputStream {
	public HDFSOutputStream(DatagridBufferedOutputStream outputStream, Statistics stats) {
		super(outputStream, stats);
	}
}