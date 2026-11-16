package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.AbstractNotableResolver;
import org.springframework.stereotype.Component;

@Component
class PodcastNotableResolver extends AbstractNotableResolver<Podcast> {

	private final PodcastService podcastService;

	PodcastNotableResolver(PodcastService podcastService) {
		super(Podcast.class);
		this.podcastService = podcastService;
	}

	@Override
	public Podcast find(Long key) {
		return this.podcastService.getPodcastById(key);
	}

	@Override
	public Long mogulId(Podcast notable) {
		return notable.mogulId();
	}

}
