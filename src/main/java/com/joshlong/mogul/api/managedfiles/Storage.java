package com.joshlong.mogul.api.managedfiles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.unit.DataSize;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

@Component
class Storage {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final S3Client s3;

	public Storage(S3Client s3) {
		this.s3 = s3;
	}

	public void remove(URI uri) {
		this.validUri(uri);
		this.remove(uri.getHost(), uri.getPath());
	}

	public void remove(String bucket, String objectName) {
		if (this.bucketExists(bucket)) {
			var delete = DeleteObjectRequest.builder().bucket(bucket).key(objectName).build();
			this.s3.deleteObject(delete);
		}
	}

	public void write(URI uri, Resource resource, MediaType mediaType) {
		this.validUri(uri);
		this.write(uri.getHost(), uri.getPath(), resource, mediaType);
	}

	/*
	 * writes N-mb sized chunks at a time to s3
	 */
	private void doWriteForLargeFiles(String bucketName, String keyName, Resource resource, DataSize maxSize,
			MediaType mediaType) throws Exception {
		try (var inputStream = new BufferedInputStream(resource.getInputStream());) {
			var chunkSize = (int) maxSize.toBytes();
			var builder = CreateMultipartUploadRequest.builder().bucket(bucketName).key(keyName);
			if (mediaType != null)
				builder = builder.contentType(mediaType.toString());
			var createMultipartUploadRequest = builder.build();
			var response = this.s3.createMultipartUpload(createMultipartUploadRequest);
			var uploadId = response.uploadId();
			var completedParts = new ArrayList<CompletedPart>();
			var partNumber = 1;
			var buffer = new byte[chunkSize];
			var bytesRead = -1;
			while ((bytesRead = inputStream.read(buffer)) > 0) {
				this.log.trace("uploading part [{}]", partNumber);
				var actualBytes = bytesRead == chunkSize ? buffer : Arrays.copyOf(buffer, bytesRead);
				var uploadPartRequest = UploadPartRequest.builder()
					.bucket(bucketName)
					.key(keyName)
					.uploadId(uploadId)
					.partNumber(partNumber)
					.build();
				var etag = this.s3.uploadPart(uploadPartRequest, RequestBody.fromBytes(actualBytes)).eTag();
				completedParts.add(CompletedPart.builder().partNumber(partNumber).eTag(etag).build());
				partNumber++;
				if (actualBytes != buffer) {
					buffer = new byte[chunkSize];
				}
			}
			var completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts).build();
			var completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
				.bucket(bucketName)
				.key(keyName)
				.uploadId(uploadId)
				.multipartUpload(completedMultipartUpload)
				.build();
			this.s3.completeMultipartUpload(completeMultipartUploadRequest);
		}
	}

	public void write(String bucket, String objectName, Resource resource, MediaType mediaType) {
		try {
			var largeFile = DataSize.ofMegabytes(10);
			this.log.info("started executing an S3 PUT for [{}/{}] on thread [{}]", bucket, objectName,
					Thread.currentThread());
			this.ensureBucketExists(bucket);
			this.doWriteForLargeFiles(bucket, objectName, resource, largeFile, mediaType);
		} //
		catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		}

	}

	private boolean bucketExists(String bucketName) {
		var buckets = this.s3.listBuckets();
		if (buckets.hasBuckets()) {
			return buckets.buckets().stream().anyMatch(bucket -> bucket.name().equalsIgnoreCase(bucketName));
		}
		return false;
	}

	/* much faster than downloading the bytes and trying to write them back up again! */
	public void copy(String src, String dest, String key, MediaType newContentType) {
		var copyRequestBuilder = CopyObjectRequest.builder()
			.metadataDirective("REPLACE")
			.sourceBucket(src)
			.sourceKey(key)
			.destinationBucket(dest)
			.destinationKey(key);
		copyRequestBuilder = newContentType == null ? copyRequestBuilder
				: copyRequestBuilder.contentType(newContentType.toString());
		var result = copyRequestBuilder.build();
		this.s3.copyObject(result);
	}

	public boolean exists(String bucket, String key) {
		var request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
		try {
			this.s3.headObject(request);
			return true;
		}
		catch (Throwable throwable) {
			return false;
		}
	}

	public Resource read(String bucket, String objectName) {
		try {
			var getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(objectName).build();
			var inputStream = this.s3.getObject(getObjectRequest);
			return new InputStreamResource(new BufferedInputStream(inputStream));
		}
		catch (Throwable throwable) {
			log.warn("error when reading bucket [{}] and object name [{}] from S3", bucket, objectName);
			return null;
		}
	}

	private void validUri(URI uri) {
		Assert.state(uri != null && uri.getScheme().equalsIgnoreCase("s3") && uri.getPath().split("/").length == 2,
				"this uri [" + Objects.requireNonNull(uri) + "] is not a valid s3 reference");
	}

	private void ensureBucketExists(String bucketName) {
		if (bucketExists(bucketName)) {
			this.log.trace("the bucket named [{}] already exists", bucketName);
			return;
		}
		this.log.info("attempting to create the bucket called [{}]", bucketName);
		this.s3.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
	}

}
