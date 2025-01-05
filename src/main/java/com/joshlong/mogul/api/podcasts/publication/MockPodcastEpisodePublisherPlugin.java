package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.podcasts.Episode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

import static com.joshlong.mogul.api.podcasts.publication.MockPodcastEpisodePublisherPlugin.PLUGIN_NAME;

/**
 * this exists only to make it easier to debug the publication process.
 */
@Component(PLUGIN_NAME)
class MockPodcastEpisodePublisherPlugin implements PodcastEpisodePublisherPlugin {

	static final String PLUGIN_NAME = "mock";

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public boolean unpublish(Map<String, String> context, Publication publication) {
		return true;
	}

	@Override
	public String name() {
		return PLUGIN_NAME;
	}

	@Override
	public Set<String> getRequiredSettingKeys() {
		return Set.of();
	}

	@Override
	public boolean isConfigurationValid(Map<String, String> context) {
		return true;
	}

	@Override
	public boolean canPublish(Map<String, String> context, Episode payload) {
		return true;
	}

	@Override
	public void publish(Map<String, String> context, Episode payload) {

		if (log.isDebugEnabled())
			log.debug("start: publishing episode {} with context {}", payload.id(), context);
		try {
			Thread.sleep(20_000);
		} //
		catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		if (log.isDebugEnabled())
			log.debug("middle: publishing episode {} with context {}", payload.id(), context);

		context.put(PublisherPlugin.CONTEXT_URL, "https://spring.io/");

		if (log.isDebugEnabled())
			log.debug("stop: publishing episode {} with context {}", payload.id(), context);
	}

}
