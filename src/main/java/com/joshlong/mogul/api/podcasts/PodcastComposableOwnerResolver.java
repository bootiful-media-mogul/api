package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.compositions.ComposableOwnerResolver;
import org.springframework.stereotype.Component;

@Component
class PodcastComposableOwnerResolver implements ComposableOwnerResolver {

	private final PodcastService podcastService;

	PodcastComposableOwnerResolver(PodcastService podcastService) {
		this.podcastService = podcastService;
	}

	@Override
	public boolean supports(Class<?> composableClass) {
		return Episode.class.equals(composableClass) || Podcast.class.equals(composableClass);
	}

	@Override
	public Long mogulId(Class<?> composableClass, Long compositionKey) {
		var podcastId = compositionKey;
		if (Episode.class.equals(composableClass)) {
			var episode = this.podcastService.getPodcastEpisodeById(compositionKey);
			if (episode == null)
				return null;
			podcastId = episode.podcastId();
		}
		var podcast = this.podcastService.getPodcastById(podcastId);
		return podcast == null ? null : podcast.mogulId();
	}

}
