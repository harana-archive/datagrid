package com.harana.datagrid.datanode.object;

import com.amazonaws.*;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class S3Test {
	private static final Logger logger = LogManager.getLogger();

	private static final String bucket = "test-bucket-" + (new Random().nextInt() % 1000);
	private static String accessKey;
	private static String secretKey;
	private static String endpoint;
	private static String protocol;

	@BeforeClass
	public static void setup() throws Exception {
		Random rand = new Random();
		System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");
		accessKey = System.getProperty("S3_ACCESS_KEY");
		if (accessKey == "" || accessKey == null) {
			throw new IllegalArgumentException("S3 access key not specified");
		}
		secretKey = System.getProperty("S3_SECRET_KEY");
		if (secretKey == "" || secretKey == null) {
			throw new IllegalArgumentException("S3 secretKey key not specified");
		}
		endpoint = System.getProperty("S3_ENDPOINT");
		if (endpoint == "" || endpoint == null) {
			throw new IllegalArgumentException("S3 endpoit not specified");
		}
		protocol = System.getProperty("S3_PROTOCOL", "HTTP");
		//BasicConfigurator.configure();
	}

	AmazonS3Client get_new_connection() {
		logger.debug("Creating S3 connection using AccessKey=" + accessKey + " SecretKey=" + secretKey);

		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		ClientConfiguration clientConf = new ClientConfiguration();
		clientConf.setProtocol(Protocol.valueOf(protocol));
		//clientConf.withSignerOverride(null);
		//clientConf.setSignerOverride(null);
		//clientConf.withSignerOverride("S3SignerType");
		//clientConf.withSignerOverride("NoOpSignerType");
		clientConf.setSocketBufferSizeHints(64 * 1024, 64 * 1024);
		clientConf.withUseExpectContinue(true);
		clientConf.withThrottledRetries(false);
		clientConf.setSocketTimeout(3000000);
		clientConf.setConnectionTimeout(3000000);
		ApacheHttpClientConfig httpConf = clientConf.getApacheHttpClientConfig();
		AmazonS3Client connection = new AmazonS3Client(credentials, clientConf);
		connection.setEndpoint(endpoint);
		return connection;
	}

	@Test
	public void t1S3ListBuckets() throws Exception {
		// List buckets
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("Listing existing buckets");
		AmazonS3Client conn = get_new_connection();
		try {
			List<Bucket> buckets = conn.listBuckets();
			for (Bucket b : buckets) {
				logger.debug("Bucket {} \t {}", b.getName(), StringUtils.fromDate(b.getCreationDate()));
			}
		} catch (Exception e) {
			logger.error("listBuckets() got exception: ", e);
			assertTrue("Could not list buckets", false);
		}
		assertTrue("List buckets successful", true);
	}


	@Test
	public void t2S3CreateBucket() throws Exception {
		// Create bucket if it does not exist
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("Creating new bucket {}", bucket);
		AmazonS3Client conn = get_new_connection();
		if (conn.doesBucketExist(bucket)) {
			logger.error("Bucket {} already exists", bucket);
		} else {
			try {
				Bucket b = conn.createBucket(bucket);
				logger.debug("Bucket {} created on {} ", b.getName(), b.getCreationDate());
			} catch (Exception e) {
				logger.error("createBucket() got exception: ", e);
				assertTrue("Bucket " + bucket + " does not exist and could not create it", false);
			}
		}
		assertTrue("Create bucket successful", true);
	}


	@Test
	public void t3S3ListObjects() throws Exception {
		// List bucket contents
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("Listing contents of bucket {}", bucket);
		AmazonS3Client conn = get_new_connection();
		try {
			ObjectListing objects = conn.listObjects(bucket);
			do {
				for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
					System.out.println(objectSummary.getKey() + "\t" + objectSummary.getSize() + "\t" +
							StringUtils.fromDate(objectSummary.getLastModified()));
				}
				objects = conn.listNextBatchOfObjects(objects);
			} while (objects.isTruncated());
		} catch (Exception e) {
			logger.error("listObjects() got exception: ", e);
			assertTrue("Could not list objects", false);
		}
		assertTrue("List objects successful", true);
	}


	@Test
	public void t4S3OneObject() throws Exception {
		String key = "TestKey";

		// Put one object
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("Putting object {}", key);
		AmazonS3Client conn = get_new_connection();
		try {
			ByteArrayInputStream buffer = new ByteArrayInputStream("Hello World!".getBytes());
			int length = buffer.available();
			ObjectMetadata md = new ObjectMetadata();
			md.setContentType("application/octet-stream");
			md.setContentLength(length);
			PutObjectRequest putReg = new PutObjectRequest(bucket, "Test", buffer, md);
			conn.putObject(putReg);
		} catch (Exception e) {
			logger.error("putObject() got exception: ", e);
			assertTrue("Could not put new object", false);
		}

		// Read back object
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("Reading back object");
		try {
			GetObjectRequest getReq = new GetObjectRequest(bucket, "Test");
			S3Object object = conn.getObject(getReq);
			logger.debug("Got back object {}={}", key, object.getObjectContent());
			//assertTrue("Object get was successful", true);
		} catch (Exception e) {
			logger.error("getObject() got exception: ", e);
			assertTrue("Could retrieve object", false);
		}

		// Delete object
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("Deleting object");
		try {
			DeleteObjectRequest delReq = new DeleteObjectRequest(bucket, "Test");
			conn.deleteObject(delReq);
			logger.debug("Deleted object Test");
		} catch (Exception e) {
			logger.error("deleteObject() got exception: ", e);
			assertTrue("Could retrieve object", false);
		}
		assertTrue("Object put/get/delete successful", true);
	}


	@Test
	public void t5S3MultipleObjects() throws Exception {
		String prefix = "TestObject-#";
		int objects = 10;

		// Put multiple objects
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("Putting multiple objects");
		AmazonS3Client conn = get_new_connection();
		for (int i = 0; i < objects; i++) {
			try {
				ByteArrayInputStream buffer = new ByteArrayInputStream(("Hello World! - #" + i).getBytes());
				String key = prefix + i;
				int length = buffer.available();
				ObjectMetadata md = new ObjectMetadata();
				md.setContentType("application/octet-stream");
				md.setContentLength(length);
				PutObjectRequest putReg = new PutObjectRequest(bucket, key, buffer, md);
				conn.putObject(putReg);
			} catch (Exception e) {
				logger.error("putObject() got exception: ", e);
				assertTrue("Could not put new object", false);
			}
		}

		// Listing recursively all objects
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("List objects with prefix");
		List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
		int count = 0;
		try {
			ListObjectsRequest listReq = new ListObjectsRequest().withBucketName(bucket).withPrefix(prefix);
			ObjectListing objectListing = null;
			do {
				objectListing = conn.listObjects(listReq);
				for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
					keys.add(new DeleteObjectsRequest.KeyVersion(objectSummary.getKey()));
					count++;
				}
				listReq.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());
		} catch (AmazonServiceException ase) {
			throw new AmazonServiceException(String.format("Failed to list files with prefix : %s from bucket : %s ",
					prefix, bucket), ase);
		} catch (AmazonClientException ace) {
			throw new AmazonClientException(String.format("Failed to delete files with prefix : %s from bucket : %s ",
					prefix, bucket), ace);
		} catch (Exception e) {
			throw new RuntimeException(String.format("Failed to delete files with prefix : %s from bucket : %s ",
					prefix, bucket), e);
		}
		assertEquals(count, objects);

		// Deleting all objects
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("Deleting all objects");
		count = 0;
		try {
			DeleteObjectsRequest delReq = new DeleteObjectsRequest(bucket);
			delReq.setKeys(keys);
			DeleteObjectsResult delObjRes = conn.deleteObjects(delReq);
			List<DeleteObjectsResult.DeletedObject> delObjs = delObjRes.getDeletedObjects();
			for (DeleteObjectsResult.DeletedObject o : delObjs) {
				count++;
			}
		} catch (Exception e) {
			logger.error("DeleteObjectsRequest() got exception: ", e);
			assertTrue("Could not delete objects", false);
		}
		assertEquals(count, objects);

		assertTrue("Multiple object operations successful", true);
	}


	@Test
	public void t6S3DeleteBucket() throws Exception {
		AmazonS3Client conn = get_new_connection();

		// Delete bucket
		logger.info("--------------------------------------------------------------------------------------------------");
		logger.info("Delete bucket");
		try {
			conn.deleteBucket(bucket);
		} catch (Exception e) {
			logger.error("DeleteObjectsRequest() got exception: ", e);
			assertTrue("Could not delete bucket " + bucket, false);
		}
		assertTrue("Delete bucket successful", true);
	}
}
