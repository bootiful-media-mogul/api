package com.joshlong.mogul.api.podcasts.search;

import com.joshlong.mogul.api.EntityContext;
import com.joshlong.mogul.api.EntityContextBuilder;
import com.joshlong.mogul.api.podcasts.Podcast;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.utils.TypeUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;

@Component
class PodcastEntityContextBuilder implements EntityContextBuilder<Podcast> {

	private final PodcastService podcastService;

	PodcastEntityContextBuilder(PodcastService podcastService) {
		this.podcastService = podcastService;
	}

	@Override
	public EntityContext buildContextFor(Long mogulId, Long podcast) {
		var found = this.podcastService.getPodcastById(podcast);
		Assert.state(found != null && found.mogulId().equals(mogulId),
				() -> "there is no podcast #" + podcast + " for mogul #" + mogulId);
		return new EntityContext(TypeUtils.typeName(Podcast.class), Map.of("podcastId", podcast));
	}

}
