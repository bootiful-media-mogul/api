package com.joshlong.mogul.api;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;

@ConfigurationProperties(prefix = "mogul")
public record ApiProperties(Aws aws, Podcasts podcasts, Transcription transcription, Settings settings, boolean debug) {

	public record Transcription(S3 s3) {

		public record S3(String inputBucket, String outputBucket) {
		}
	}

	public record Settings(String password, String salt) {
	}

	public record Aws(String accessKey, String accessKeySecret, String region, Cloudfront cloudfront) {
		public record Cloudfront(URI domain) {
		}
	}

	public record Podcasts(Producer production) {

		public record Producer(Amqp amqp) {

			public record Amqp(String requests, String replies) {
			}

		}
	}
}