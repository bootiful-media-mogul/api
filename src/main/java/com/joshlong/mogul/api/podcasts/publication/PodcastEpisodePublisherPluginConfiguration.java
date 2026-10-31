package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.podcasts.PodcastService;
import org.springframework.context.ApplicationEventPublisher;

import com.joshlong.mogul.api.podcasts.production.PodcastProducer;
import com.joshlong.mogul.api.publications.PublicationService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class PodcastEpisodePublisherPluginConfiguration {

	@Bean
	ProducedAudioPublicationGate producedAudioPublicationGate(PodcastProducer podcastProducer,
			PublicationService publicationService, PodcastService podcastService, ApplicationEventPublisher publisher) {
		return new ProducedAudioPublicationGate(podcastProducer, publicationService, podcastService, publisher);
	}

}
