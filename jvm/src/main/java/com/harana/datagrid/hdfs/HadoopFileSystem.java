package com.harana.datagrid.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import com.harana.datagrid.*;
import com.harana.datagrid.client.namenode.NamenodeErrors;
import com.harana.datagrid.conf.DatagridConfiguration;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.core.streams.DatagridBufferedInputStream;
import com.harana.datagrid.core.streams.DatagridBufferedOutputStream;
import com.harana.datagrid.data.DatagridDirectory;
import com.harana.datagrid.data.DatagridFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HadoopFileSystem extends FileSystem {
	private static final Logger logger = LogManager.getLogger();
	private Datagrid dfs;
	private Path workingDir;
	private URI uri;
	
	public HadoopFileSystem() {
		logger.info("HadoopFileSystem construction");
		dfs = null;
	}
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
		
		try {
			DatagridConfiguration crailConf = DatagridConfiguration.createConfigurationFromFile();
			this.dfs = Datagrid.newInstance(crailConf);
			Path _workingDir = new Path("/user/" + DatagridConstants.USER);
			this.workingDir = new Path("/user/" + DatagridConstants.USER).makeQualified(uri, _workingDir);
			this.uri = URI.create(DatagridConstants.NAMENODE_ADDRESS);
			logger.info("HadoopFileSystem fs initialization done..");
		} catch(Exception e) {
			throw new IOException(e);
		}
	}
	
	public String getScheme() {
		return "crail";
	}

	public URI getUri() {
		return uri;
	}	

	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		statistics.incrementReadOps(1);
		DatagridFile fileInfo;
		try {
			fileInfo = dfs.lookup(path.toUri().getRawPath()).get().asFile();
			DatagridBufferedInputStream inputStream = fileInfo.getBufferedInputStream(fileInfo.getCapacity());
			return new HDFSInputStream(inputStream, statistics);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public FSDataOutputStream create(Path path, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		statistics.incrementWriteOps(1);
		DatagridFile fileInfo;
		try {
			fileInfo = dfs.create(path.toUri().getRawPath(), DatagridDataType.DATAFILE, DatagridStorageClass.PARENT, DatagridLocationClass.PARENT, true).get().asFile();
		} catch (Exception e) {
			if (e.getMessage().contains(NamenodeErrors.messages[NamenodeErrors.ERR_PARENT_MISSING])) fileInfo = null;
			else {
				throw new IOException(e);
			}
		}
		
		if (fileInfo == null) {
			Path parent = path.getParent();
			this.mkdirs(parent, FsPermission.getDirDefault());
			try {
				fileInfo = dfs.create(path.toUri().getRawPath(), DatagridDataType.DATAFILE, DatagridStorageClass.PARENT, DatagridLocationClass.PARENT, true).get().asFile();
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		DatagridBufferedOutputStream datagridOutputStream = null;
		if (fileInfo != null) {
			try {
				fileInfo.syncDir();
				datagridOutputStream = fileInfo.getBufferedOutputStream(Integer.MAX_VALUE);
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
		
		if (datagridOutputStream != null) {
			return new HDFSOutputStream(datagridOutputStream, statistics);
		} else {
			throw new IOException("Failed to create file, path " + path.toString());
		}
	}

	@Override
	public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
		throw new IOException("Append not supported");
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		try {
			statistics.incrementWriteOps(1);
			DatagridData file = dfs.rename(src.toUri().getRawPath(), dst.toUri().getRawPath()).get();
			if (file != null) {
				file.syncDir();
			}
			return file != null;
		} catch(Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean delete(Path path, boolean recursive) throws IOException {
		try {
			statistics.incrementWriteOps(1);
			DatagridData file = dfs.delete(path.toUri().getRawPath(), recursive).get();
			if (file != null) {
				file.syncDir();
			}
			return file != null;
		} catch(Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public FileStatus[] listStatus(Path path) throws IOException {
		try {
			DatagridData node = dfs.lookup(path.toUri().getRawPath()).get();
			Iterator<String> iter = node.asContainer().listEntries();
			ArrayList<FileStatus> statusList = new ArrayList<>();
			while (iter.hasNext()) {
				String filepath = iter.next();
				DatagridData directFile = dfs.lookup(filepath).get();
				if (directFile != null) {
					FsPermission permission = FsPermission.getFileDefault();
					if (directFile.getType().isDirectory()) {
						permission = FsPermission.getDirDefault();
					}
					FileStatus status = new FileStatus(directFile.getCapacity(), directFile.getType().isContainer(), DatagridConstants.SHADOW_REPLICATION, DatagridConstants.BLOCK_SIZE, directFile.getModificationTime(), directFile.getModificationTime(), permission, DatagridConstants.USER, DatagridConstants.USER, new Path(filepath).makeQualified(this.getUri(), this.workingDir));
					statusList.add(status);
				}
			}
			FileStatus[] list = new FileStatus[statusList.size()];
			statusList.toArray(list);
			return list;
		} catch(Exception e) {
			throw new FileNotFoundException(path.toUri().getRawPath());
		}
	}

	@Override
	public void setWorkingDirectory(Path new_dir) {
		this.workingDir = new_dir;
	}

	@Override
	public Path getWorkingDirectory() {
		return this.workingDir;
	}

	@Override
	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
		try {
			statistics.incrementWriteOps(1);
			DatagridDirectory data = dfs.create(path.toUri().getRawPath(), DatagridDataType.DIRECTORY, DatagridStorageClass.PARENT, DatagridLocationClass.DEFAULT, true).get().asDirectory();
			data.syncDir();
			return true;
		} catch(Exception e) {
			if (e.getMessage().contains(NamenodeErrors.messages[NamenodeErrors.ERR_PARENT_MISSING])) {
				Path parent = path.getParent();
				mkdirs(parent);
				return mkdirs(path);
			} else if (e.getMessage().contains(NamenodeErrors.messages[NamenodeErrors.ERR_FILE_EXISTS])) {
				return true;
			} else {
				throw new IOException(e);
			}
		}
	}

	@Override
	public FileStatus getFileStatus(Path path) throws IOException {
		statistics.incrementReadOps(1);
		DatagridData directFile;
		try {
			directFile = dfs.lookup(path.toUri().getRawPath()).get();
		} catch (Exception e) {
			throw new IOException(e);
		}
		if (directFile == null) {
			throw new FileNotFoundException("File does not exist: " + path);
		}
		FsPermission permission = FsPermission.getFileDefault();
		if (directFile.getType().isDirectory()) {
			permission = FsPermission.getDirDefault();
		}
		return new FileStatus(directFile.getCapacity(), directFile.getType().isContainer(), DatagridConstants.SHADOW_REPLICATION, DatagridConstants.BLOCK_SIZE, directFile.getModificationTime(), directFile.getModificationTime(), permission, DatagridConstants.USER, DatagridConstants.USER, path.makeQualified(this.getUri(), this.workingDir));
	}

	@Override
	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {
		return this.getFileBlockLocations(file.getPath(), start, len);
	}

	@Override
	public BlockLocation[] getFileBlockLocations(Path path, long start, long len) throws IOException {
		try {
			statistics.incrementReadOps(1);
			DatagridBlockLocation[] _locations = dfs.lookup(path.toUri().getRawPath()).get().asFile().getBlockLocations(start, len);
			BlockLocation[] locations = new BlockLocation[_locations.length];
			for (int i = 0; i < locations.length; i++) {
				locations[i] = new BlockLocation();
				locations[i].setOffset(_locations[i].getOffset());
				locations[i].setLength(_locations[i].getLength());
				locations[i].setNames(_locations[i].getNames());
				locations[i].setHosts(_locations[i].getHosts());
				locations[i].setTopologyPaths(_locations[i].getTopology());
			}			
			return locations;
		} catch(Exception e) {
			throw new IOException(e);
		}
	}
	
	@Override
	public FsStatus getStatus(Path p) throws IOException {
		statistics.incrementReadOps(1);
		return new FsStatus(Long.MAX_VALUE, 0, Long.MAX_VALUE);
	}
	
	@Override
	public void close() throws IOException {
		try {
			logger.info("Closing HadoopFileSystem");
			super.processDeleteOnExit();
			dfs.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
}

