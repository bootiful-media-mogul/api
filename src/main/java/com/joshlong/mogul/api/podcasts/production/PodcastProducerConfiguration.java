package com.joshlong.mogul.api.podcasts.production;

import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.podcasts.PodcastService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@Configuration
class PodcastProducerConfiguration {

	@Bean
	PodcastProducer podcastProducer(AudioEncoder audioEncoder, ManagedFileService managedFileService,
			PodcastService podcastService, @Value("${mogul.podcasts.pipeline.root}") File root) {
		return new PodcastProducer(audioEncoder, managedFileService, podcastService, root);
	}

}
