package com.joshlong.mogul.api.compositions;

import com.joshlong.mogul.api.podcasts.PodcastService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
@Transactional
@SpringBootTest
class DefaultCompositionServiceTest {

	private final JdbcClient db;

	private final PodcastService podcastService;

	DefaultCompositionServiceTest(@Autowired JdbcClient db, @Autowired PodcastService podcastService) {
		this.db = db;
		this.podcastService = podcastService;
	}

	@BeforeAll
	static void reset(@Autowired JdbcClient db) {
		db.sql("delete from composition_attachment ").update();
		db.sql("delete from composition").update();
	}

	@Test
	void composeAndAttach() {
		var mogulId = this.db.sql(" select id from mogul limit 1 ").query(Long.class).single();

		var podcast = this.podcastService.createPodcast(mogulId, "the simplest podcast ever");
		var episode = this.podcastService.createPodcastEpisodeDraft(mogulId, podcast.id(), "the title",
				"the description");
		assertEquals(episode.id(), episode.compositionKey());
		var descriptionComp = this.podcastService.getPodcastEpisodeDescriptionComposition(episode.id());
		var titleComp = this.podcastService.getPodcastEpisodeTitleComposition(episode.id());
		assertNotNull(descriptionComp, "the composition for the description must be non-null");
		assertNotNull(titleComp, "the composition for the title must be non-null");

	}

}