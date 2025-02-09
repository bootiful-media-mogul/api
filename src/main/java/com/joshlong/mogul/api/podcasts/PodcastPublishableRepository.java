package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.PublishableRepository;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
class PodcastPublishableRepository implements PublishableRepository<Episode> {

	private final PodcastService podcastService;

	PodcastPublishableRepository(PodcastService podcastService) {
		this.podcastService = podcastService;
	}

	@Override
	public Episode find(Serializable serializable) {
		return podcastService.getPodcastEpisodeById((Long) serializable);
	}

	@Override
	public boolean supports(Class<?> clazz) {
		return Episode.class.isAssignableFrom(clazz);
	}

}
