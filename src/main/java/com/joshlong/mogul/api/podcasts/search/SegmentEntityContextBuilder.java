package com.joshlong.mogul.api.podcasts.search;

import com.joshlong.mogul.api.EntityContext;
import com.joshlong.mogul.api.EntityContextBuilder;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.podcasts.Segment;
import com.joshlong.mogul.utils.TypeUtils;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
class SegmentEntityContextBuilder implements EntityContextBuilder<Segment> {

	private final PodcastService podcastService;

	SegmentEntityContextBuilder(PodcastService podcastService) {
		this.podcastService = podcastService;
	}

	@Override
	public EntityContext buildContextFor(Long mogul, Long segmentId) {
		// asserts that the segment is the mogul's
		var segment = this.podcastService.getPodcastEpisodeSegmentById(mogul, segmentId);
		var episodes = this.podcastService.getAllPodcastEpisodesByIds(List.of(segment.episodeId()));
		var episode = episodes.iterator().next();
		return new EntityContext(TypeUtils.typeName(Segment.class),
				Map.of("episodeId", episode.id(), "segmentId", segment.id(), "podcastId", episode.podcastId()));
	}

}
