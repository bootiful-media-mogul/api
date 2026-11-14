package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.AbstractPublishableResolver;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Component
class PodcastPublishableResolver extends AbstractPublishableResolver<Episode> {

	private final PodcastService podcastService;

	PodcastPublishableResolver(PodcastService podcastService) {
		super(Episode.class);
		this.podcastService = podcastService;
	}

	@Override
	public Episode find(Long id) {
		return podcastService.getPodcastEpisodeById(id);
	}

	@Override
	public Map<Long, Episode> findAll(Collection<Long> keys) {
		var map = new HashMap<Long, Episode>();
		for (var episode : this.podcastService.getAllPodcastEpisodesByIds(keys))
			map.put(episode.id(), episode);
		return map;
	}

	@Override
	public String title(Episode publishable) {
		return publishable.title();
	}

}
