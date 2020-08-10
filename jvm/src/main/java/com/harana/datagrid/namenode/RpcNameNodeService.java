package com.harana.datagrid.namenode;

public interface RpcNameNodeService {
	
	short createFile(NamenodeRequest.CreateFile request, NamenodeResponse.CreateFile response, NamenodeState errorState) throws Exception;
	short getFile(NamenodeRequest.GetFile request, NamenodeResponse.GetFile response, NamenodeState errorState) throws Exception;
	short setFile(NamenodeRequest.SetFile request, NamenodeResponse.Void response, NamenodeState errorState) throws Exception;
	short removeFile(NamenodeRequest.RemoveFile request, NamenodeResponse.DeleteFile response, NamenodeState errorState) throws Exception;
	short renameFile(NamenodeRequest.RenameFile request, NamenodeResponse.RenameFile response, NamenodeState errorState) throws Exception;
	short getDataNode(NamenodeRequest.GetDataNode request, NamenodeResponse.GetDataNode response, NamenodeState errorState) throws Exception;
	short setBlock(NamenodeRequest.SetBlock request, NamenodeResponse.Void response, NamenodeState errorState) throws Exception;
	short getBlock(NamenodeRequest.GetBlock request, NamenodeResponse.GetBlock response, NamenodeState errorState) throws Exception;
	short getLocation(NamenodeRequest.GetLocation request, NamenodeResponse.GetLocation response, NamenodeState errorState) throws Exception;
	short dump(NamenodeRequest.DumpNameNode request, NamenodeResponse.Void response, NamenodeState errorState) throws Exception;
	short ping(NamenodeRequest.PingNameNode request, NamenodeResponse.PingNameNode response, NamenodeState errorState) throws Exception;
	
	@SuppressWarnings("unchecked")
	static RpcNameNodeService createInstance(String name) throws Exception {
		Class<?> serviceClass = Class.forName(name);
		if (RpcNameNodeService.class.isAssignableFrom(serviceClass)) {
			Class<? extends RpcNameNodeService> rpcService = (Class<? extends RpcNameNodeService>) serviceClass;
			return rpcService.newInstance();
		} else {
			throw new Exception("Cannot instantiate RPC service of type " + name);
		}
	}	
}