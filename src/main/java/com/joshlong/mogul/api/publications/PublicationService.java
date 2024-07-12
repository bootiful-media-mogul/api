package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.PublisherPlugin;

import java.util.Map;

/**
 * handles preparing context, launching the {@link PublisherPlugin}, and noting the
 * publication in the DB.
 */
public interface PublicationService {

	Publication getPublicationById(Long id);

	<T extends Publishable> Publication publish(Long mogulId, T payload, Map<String, String> contextAndSettings,
			PublisherPlugin<T> plugin);

}
