package com.harana.datagrid.datanode.object.client;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.harana.datagrid.conf.DatagridConstants;
import com.harana.datagrid.datanode.object.ObjectStoreConstants;
import com.harana.datagrid.datanode.object.ObjectStoreUtils;
import com.harana.datagrid.DatagridBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class S3ObjectStoreClient {
	private static final Logger logger = LogManager.getLogger();

	private final AmazonS3Client[] connections;
	private final ConcurrentHashMap<Long, ObjectMetadata> objectMetadata;

	public S3ObjectStoreClient() {
		System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
		AWSCredentials credentials = new BasicAWSCredentials(ObjectStoreConstants.S3_ACCESS,
				ObjectStoreConstants.S3_SECRET);
		logger.debug("Using S3 credentials AccessKey={} SecretKey={}",
				ObjectStoreConstants.S3_ACCESS, ObjectStoreConstants.S3_SECRET);

		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.setProtocol(Protocol.valueOf(ObjectStoreConstants.S3_PROTOCOL));
		clientConf.setSocketBufferSizeHints(64 * 1024, 64 * 1024);
		clientConf.withUseExpectContinue(true);
		clientConf.withThrottledRetries(false);
		clientConf.setSocketTimeout(3000000);
		clientConf.setConnectionTimeout(3000000);
		String[] endpoints = ObjectStoreConstants.S3_ENDPOINT.split(",|;");
		if (ObjectStoreConstants.S3_SIGNER != null) {
			clientConf.setSignerOverride(ObjectStoreConstants.S3_SIGNER);
		}
		connections = new AmazonS3Client[endpoints.length];
		int i = 0;
		for (String endpoint : endpoints) {
			connections[i] = new AmazonS3Client(credentials, clientConf);
			logger.debug("Creating new connection to S3 endpoint {}", endpoint);
			connections[i].setEndpoint(endpoint);
			if (ObjectStoreConstants.S3_REGION_NAME != null) {
				connections[i].setRegion(Region.getRegion(Regions.fromName(ObjectStoreConstants.S3_REGION_NAME)));
			}
			i++;
		}
		objectMetadata = new ConcurrentHashMap<>();
		logger.debug("Successfully created S3Client");
	}

	public boolean runBasicTests(AmazonS3Client conn) {
		logger.debug("--------------------------------------------------------------------------------------------------");
		logger.debug("Test S3 connection by writing and reading an object");
		logger.debug("--------------------------------------------------------------------------------------------------");
		try {
			ByteArrayInputStream buffer = new ByteArrayInputStream("Hello World!".getBytes());
			int length = buffer.available();
			ObjectMetadata md = new ObjectMetadata();
			md.setContentType("application/octet-stream");
			md.setContentLength(length);
			PutObjectRequest putReg = new PutObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, "Test", buffer, md);
			conn.putObject(putReg);
		} catch (Exception e) {
			logger.error("putObject() got exception: ", e);
			return false;
		}
		try {
			GetObjectRequest getReq = new GetObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, "Test");
			S3Object object = conn.getObject(getReq);
			System.out.println("Got back object Test=" + object.getObjectContent());
		} catch (Exception e) {
			logger.error("getObject() got exception: ", e);
			return false;
		}
		return true;
	}

	public void close() {
		logger.info("Closing S3 Client");
	}

	private ObjectMetadata getObjectMetadata() {
		long threadId = Thread.currentThread().getId();
		ObjectMetadata md = objectMetadata.get(threadId);
		if (md == null) {
			md = new ObjectMetadata();
			//md.setUserMetadata(metadata);
			md.setContentType("application/octet-stream");
			objectMetadata.put(threadId, md);
		}
		return md;
	}

	private int getEndpoit(String key) {
		// select an endpoint based on the block ID
		return  Integer.parseInt(key.split("-")[4]) % connections.length;
	}

	public InputStream getObject(String key) throws AmazonClientException {
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		GetObjectRequest objReq = new GetObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key);
		S3Object object = connection.getObject(objReq);
		return object.getObjectContent();
	}

	public InputStream getObject(String key, long startOffset, long endOffset) throws AmazonClientException {
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		GetObjectRequest objReq = new GetObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key);
		logger.debug("TID {} : Getting object {}, start offset = {}, end offset = {} ", Thread.currentThread().getId(), key, startOffset, endOffset);
		if (startOffset > 0 || endOffset != DatagridConstants.BLOCK_SIZE) {
			// NOTE: start and end offset are inclusive in the S3 API.
			objReq.withRange(startOffset, endOffset - 1);
		}
		S3Object object;
		if (ObjectStoreConstants.PROFILE) {
			long startTime = System.nanoTime();
			object = connection.getObject(objReq);
			long endTime = System.nanoTime();
			logger.debug("TID {} : S3 endpoint {} getObject() initial response took {} usec", Thread.currentThread().getId(), endpointID, (endTime - startTime) / 1000.);
		} else {
			object = connection.getObject(objReq);
		}
		return object.getObjectContent();
	}

	public void putObject(String key, DatagridBuffer buffer) throws AmazonClientException {
		int length = buffer.remaining();
		InputStream input = new ObjectStoreUtils.ByteBufferBackedInputStream(buffer);
		ObjectMetadata md = getObjectMetadata();
		md.setContentLength(length);
		PutObjectRequest request = new PutObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key, input, md);
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		if (ObjectStoreConstants.PROFILE) {
			long startTime = System.nanoTime();
			connection.putObject(request);
			long endTime = System.nanoTime();
			logger.debug("TID {} : S3 putObject() of {} bytes to endpoint {} took {} usec", Thread.currentThread().getId(), length, endpointID, (endTime - startTime) / 1000.);
		} else {
			connection.putObject(request);
		}
	}

	public boolean deleteObject(String key) {
		// The ability to delete an object is not critical for running the  object tier
		int endpointID = getEndpoit(key);
		AmazonS3 connection = connections[endpointID];
		try {
			connection.deleteObject(new DeleteObjectRequest(ObjectStoreConstants.S3_BUCKET_NAME, key));
		} catch (AmazonServiceException ase) {
			logger.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			logger.error("Error Message:    " + ase.getMessage());
			logger.error("HTTP Status Code: " + ase.getStatusCode());
			logger.error("AWS Error Code:   " + ase.getErrorCode());
			logger.error("Error Type:       " + ase.getErrorType());
			logger.error("Request ID:       " + ase.getRequestId());
			logger.error("Exception: ", ase);
			return false;
		} catch (AmazonClientException ace) {
			logger.error("AmazonClientException (the client encountered an internal error while trying to " + "communicate with the Object Store):");
			logger.error("Error Message: " + ace.getMessage());
			logger.error("Exception: ", ace);
			return false;
		} catch (Exception e) {
			logger.error("Got exception: ", e);
			return false;
		}
		return true;
	}

	public boolean createBucket(String bucketName) {
		// The ability to create a bucket is not critical for running the  object tier
		try {
			if (!(connections[0].doesBucketExist(bucketName))) {
				Bucket bucket = connections[0].createBucket(bucketName);
				String bucketLocation = "";
				try {
					bucketLocation = connections[0].getBucketLocation(bucketName);
				} catch (Exception e) {
					logger.warn("Could not get bucket {} location", bucketName, e);
				}
				logger.debug("Created new bucket {} in location {} on {}", bucket.getName(), bucketLocation, bucket.getCreationDate());
			} else {
				logger.warn("Bucket {} already exists", bucketName);
			}
		} catch (AmazonServiceException ase) {
			logger.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			logger.error("Error Message:    " + ase.getMessage());
			logger.error("HTTP Status Code: " + ase.getStatusCode());
			logger.error("AWS Error Code:   " + ase.getErrorCode());
			logger.error("Error Type:       " + ase.getErrorType());
			logger.error("Request ID:       " + ase.getRequestId());
			logger.error("Exception: ", ase);
			return false;
		} catch (AmazonClientException ace) {
			logger.error("AmazonClientException (the client encountered an internal error while trying to " + "communicate with the Object Store):");
			logger.error("Error Message: " + ace.getMessage());
			logger.error("Exception: ", ace);
			return false;
		} catch (Exception e) {
			logger.error("Got exception: ", e);
			return false;
		}
		return true;
	}

	public boolean deleteBucket(String bucketName) {
		// The ability to delete a bucket is not critical for running the  object tier
		try {
			connections[0].deleteBucket(bucketName);
			logger.debug("Deleted bucket {}", bucketName);
		} catch (AmazonServiceException ase) {
			logger.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			logger.error("Error Message:    " + ase.getMessage());
			logger.error("HTTP Status Code: " + ase.getStatusCode());
			logger.error("AWS Error Code:   " + ase.getErrorCode());
			logger.error("Error Type:       " + ase.getErrorType());
			logger.error("Request ID:       " + ase.getRequestId());
			return false;
		} catch (AmazonClientException ace) {
			logger.error("AmazonClientException (the client encountered an internal error while trying to " +
					"communicate with the Object Store):");
			logger.error("Error Message: " + ace.getMessage());
		} catch (Exception e) {
			logger.error("Got exception: ", e);
			return false;
		}
		return true;
	}

	public boolean deleteObjectsWithPrefix(String prefix) {
		// The ability to delete objects is not critical for running the  object tier
		final String bucket = ObjectStoreConstants.S3_BUCKET_NAME;
		logger.debug("Deleting all objects in bucket {} with prefix {}", bucket, prefix);

		// Find all objects with prefix
		List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
		int count = 0;
		try {
			ListObjectsRequest listReq = new ListObjectsRequest().
					withBucketName(ObjectStoreConstants.S3_BUCKET_NAME).
					withPrefix(prefix);
			ObjectListing objectListing;
			do {
				objectListing = connections[0].listObjects(listReq);
				for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
					keys.add(new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()));
					count++;
				}
				listReq.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
		} catch (AmazonServiceException ase) {
			logger.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			logger.error("Error Message:    " + ase.getMessage());
			logger.error("HTTP Status Code: " + ase.getStatusCode());
			logger.error("AWS Error Code:   " + ase.getErrorCode());
			logger.error("Error Type:       " + ase.getErrorType());
			logger.error("Request ID:       " + ase.getRequestId());
			return false;
		} catch (AmazonClientException ace) {
			logger.error("AmazonClientException (the client encountered an internal error while trying to " + "communicate with the Object Store):");
			logger.error("Error Message: " + ace.getMessage());
			return false;
		} catch (Exception e) {
			logger.error("Got exception: ", e);
			return false;
		}

		// Delete objects
		count = 0;
		try {
			DeleteObjectsRequest delReq = new DeleteObjectsRequest(bucket);
			delReq.setKeys(keys);
			DeleteObjectsResult delObjRes = connections[0].deleteObjects(delReq);
			List<DeleteObjectsResult.DeletedObject> delObjs = delObjRes.getDeletedObjects();
			for (DeleteObjectsResult.DeletedObject o : delObjs) {
				count++;
			}
		} catch (AmazonServiceException ase) {
			logger.error("AmazonServiceException (the request to the Object Store was rejected with an error message):");
			logger.error("Error Message:    " + ase.getMessage());
			logger.error("HTTP Status Code: " + ase.getStatusCode());
			logger.error("AWS Error Code:   " + ase.getErrorCode());
			logger.error("Error Type:       " + ase.getErrorType());
			logger.error("Request ID:       " + ase.getRequestId());
			return false;
		} catch (AmazonClientException ace) {
			logger.error("AmazonClientException (the client encountered an internal error while trying to " + "communicate with the Object Store):");
			logger.error("Error Message: " + ace.getMessage());
			return false;
		} catch (Exception e) {
			logger.error("Got exception: ", e);
			return false;
		}
		logger.debug("Deleted {} objects", count);
		return true;
	}
}
