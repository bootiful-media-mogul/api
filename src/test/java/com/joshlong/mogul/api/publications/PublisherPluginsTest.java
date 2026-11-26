package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.mogul.MogulStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * a client asks which plugins go with a kind of publishable, and which of those can be
 * scheduled, instead of hardcoding both. the kind is inferred from each plugin's type.
 */
@SpringBootTest
class PublisherPluginsTest {

	private static Map<String, Boolean> schedulableByName(Iterable<PublisherPluginDescriptor> descriptors) {
		var map = new java.util.HashMap<String, Boolean>();
		descriptors.forEach(d -> map.put(d.name(), d.schedulable()));
		return map;
	}

	@Test
	void pluginsAreDescribedByTheirPublishableType(@Autowired PublicationController<?> controller) {
		var episode = schedulableByName(controller.publisherPlugins("episode"));
		assertEquals(Boolean.TRUE, episode.get("podbean"));
		assertEquals(Boolean.TRUE, episode.get("podcastEpisodeAyrshare"));
		assertEquals(Boolean.TRUE, episode.get("podcastEpisodeToBlogPost"));
		assertEquals(Boolean.FALSE, episode.get("audioFile"));
		assertFalse(episode.containsKey("blogPostAyrshare"));

		var post = schedulableByName(controller.publisherPlugins("post"));
		assertEquals(Boolean.TRUE, post.get("blogPostAyrshare"));
		assertEquals(Boolean.TRUE, post.get("wordpress"));
		assertEquals(Boolean.FALSE, post.get("blogPostMarkdownFile"));

		var status = schedulableByName(controller.publisherPlugins("mogulStatus"));
		assertEquals(Map.of("mogulAyrshare", true), status);

		// no type, every plugin
		var all = controller.publisherPlugins(null)
			.stream()
			.map(PublisherPluginDescriptor::name)
			.collect(Collectors.toSet());
		assertTrue(all.containsAll(episode.keySet()));
		assertTrue(all.containsAll(post.keySet()));
		assertTrue(all.containsAll(status.keySet()));
	}

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void aPluginThatCantBeScheduledRefusesASchedule(@Autowired PublicationService publicationService,
			@Autowired Map<String, PublisherPlugin<?>> plugins) {
		var download = (PublisherPlugin) plugins.get("blogPostMarkdownFile");
		assertFalse(download.schedulable());
		// refused before anything about the payload is looked at
		var anything = new MogulStatus(1L, 1L, LocalDate.now(), new Date());
		var tomorrow = Instant.now().plus(1, ChronoUnit.DAYS);
		var e = assertThrows(IllegalStateException.class,
				() -> publicationService.publish(1L, anything, Map.of(), download, tomorrow));
		assertTrue(e.getMessage().contains("can't be scheduled"));
	}

}
