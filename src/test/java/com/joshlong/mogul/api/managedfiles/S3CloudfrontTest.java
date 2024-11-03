package com.joshlong.mogul.api.managedfiles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudfront.CloudFrontClient;
import software.amazon.awssdk.services.cloudfront.model.*;

import java.util.concurrent.atomic.AtomicReference;

class S3CloudfrontTest {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private static StaticCredentialsProvider authenticate() {
		var accessId = System.getenv("AWS_ACCESS_KEY_ID");
		var accessSecret = System.getenv("AWS_ACCESS_KEY_SECRET");
		return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessId, accessSecret));
	}

	private final AtomicReference<StaticCredentialsProvider> credentials = new AtomicReference<>(authenticate());

	// @Test
	void go() {
		this.createCloudFrontDistribution(this.credentials.get(), "mogul-podcast-episodes.s3.us-east-1.amazonaws.com",
				"S3-my-bucket");
	}

	private void createCloudFrontDistribution(AwsCredentialsProvider credentialsProvider, String s3BucketDomain,
			String originId) {
		// Create CloudFront client
		var cloudFrontClient = CloudFrontClient.builder()
			.credentialsProvider(credentialsProvider)
			.region(Region.AWS_GLOBAL)
			.build();

		try {

			var s3OriginConfig = S3OriginConfig.builder()
				.originAccessIdentity("") // Leave empty for public bucket
				.build();
			var origin = Origin.builder()
				.domainName(s3BucketDomain) // Replace with your S3 bucket domain
				.id(originId) // Unique identifier for this origin
				.s3OriginConfig(s3OriginConfig)
				.build();
			var defaultCacheBehavior = DefaultCacheBehavior.builder()
				// .cachePolicyId ("visible-s3-bucket-policy")
				// .minTTL()
				.targetOriginId(originId) // Must match origin ID above
				.viewerProtocolPolicy(ViewerProtocolPolicy.REDIRECT_TO_HTTPS)
				.allowedMethods(AllowedMethods.builder().items(Method.GET, Method.HEAD).quantity(2).build())
				.build();
			/*
			 * // Create default cache behavior
			 */

			// Create distribution config
			var distConfig = DistributionConfig.builder()
				.origins(Origins.builder().items(origin).quantity(1).build())
				.defaultCacheBehavior(defaultCacheBehavior)
				.callerReference(String.valueOf(System.currentTimeMillis()))
				.comment("Distribution for my-bucket")
				.enabled(true)
				.defaultRootObject("index.html")
				.priceClass(PriceClass.PRICE_CLASS_ALL)
				.build();

			// Create distribution request
			var createDistRequest = CreateDistributionRequest.builder().distributionConfig(distConfig).build();

			// Create the distribution
			var response = cloudFrontClient.createDistribution(createDistRequest);

			// Print the distribution details
			var distribution = response.distribution();
			this.log.info("Distribution created successfully!");
			this.log.info("Distribution ID: {}", distribution.id());
			this.log.info("Domain Name: {}", distribution.domainName());
			this.log.info("Status: {}", distribution.status());

		} //
		catch (CloudFrontException e) {
			this.log.error("Error creating CloudFront distribution: {}", e.getMessage());
			throw new IllegalArgumentException(e);
		}
	}

}