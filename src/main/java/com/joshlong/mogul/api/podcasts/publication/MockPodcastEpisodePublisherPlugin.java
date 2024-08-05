package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.podcasts.Episode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

import static com.joshlong.mogul.api.podcasts.publication.MockPodcastEpisodePublisherPlugin.PLUGIN_NAME;

@Component(PLUGIN_NAME)
class MockPodcastEpisodePublisherPlugin  implements PodcastEpisodePublisherPlugin {

    private final Logger log = LoggerFactory.getLogger(getClass());

    static final String PLUGIN_NAME = "mock";

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
            log.debug("publishing episode {} with context {}",
                    payload.id(), context);
    }
}
