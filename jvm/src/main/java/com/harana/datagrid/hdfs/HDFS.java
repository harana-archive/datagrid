package com.harana.datagrid.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

public class HDFS extends AbstractFileSystem {
	private static final Logger logger = LogManager.getLogger();
	private final HadoopFileSystem dfs;
	
	public HDFS(final URI uri, final Configuration conf) throws IOException, URISyntaxException {
		super(uri, "crail", true, 9000);
		this.dfs = new HadoopFileSystem();
		dfs.initialize(uri, conf);
	}

	@Override
	public int getUriDefaultPort() {
		return 9000;
	}

	@Override
	public FsServerDefaults getServerDefaults() throws IOException {
		return dfs.getServerDefaults(dfs.getWorkingDirectory());
	}

	@Override
	public FSDataOutputStream createInternal(Path path, EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize, short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt, boolean createParent) throws IOException {
		return dfs.create(path, absolutePermission, false, bufferSize, replication, blockSize, progress);
	}

	@Override
	public void mkdir(Path path, FsPermission permission, boolean createParent) throws IOException {
		dfs.mkdirs(path, permission);
	}

	@Override
	public boolean delete(Path path, boolean recursive) throws IOException {
		return dfs.delete(path, recursive);
	}

	@Override
	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		return dfs.open(path, bufferSize);
	}

	@Override
	public boolean setReplication(Path f, short replication) throws IOException {
		return dfs.setReplication(f, replication);
	}

	@Override
	public void renameInternal(Path src, Path dst) throws IOException {
		dfs.rename(src, dst);
	}

	@Override
	public void setPermission(Path f, FsPermission permission) throws IOException {
		dfs.setPermission(f, permission);
	}

	@Override
	public void setOwner(Path f, String username, String groupname) {
	}

	@Override
	public void setTimes(Path f, long mtime, long atime) {
	}

	@Override
	public FileChecksum getFileChecksum(Path f) {
		return null;
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		return dfs.getFileStatus(path);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws IOException {
		return dfs.getFileBlockLocations(path, start, len);
	}

	@Override
	public FsStatus getFsStatus() throws IOException {
		return dfs.getStatus();
	}

	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		return dfs.listStatus(path);
	}

	@Override
	public void setVerifyChecksum(boolean verifyChecksum) {
		dfs.setVerifyChecksum(verifyChecksum);
	}
}
