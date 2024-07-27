package com.joshlong.mogul.api;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
class AwsConfiguration {

	@Bean
	S3Client s3Client(ApiProperties api) {
		var key = api.aws().accessKey();
		var secret = api.aws().accessKeySecret();
		var creds = AwsBasicCredentials.create(key, secret);
		return S3Client.builder()
			.region(Region.of(api.aws().region()))
			.credentialsProvider(StaticCredentialsProvider.create(creds))
			.forcePathStyle(true)
			.build();
	}

	@Bean
	InitializingBean validateS3(ApiProperties properties, S3Client s3) {
		return () -> {
			if (!properties.debug())
				return;
			s3.listBuckets().buckets().forEach(System.out::println);
		};
	}

}
