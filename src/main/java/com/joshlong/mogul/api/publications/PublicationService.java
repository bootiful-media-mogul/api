package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.PublisherPlugin;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * handles preparing context, launching the {@link PublisherPlugin}, and noting the
 * publication in the DB.
 */
public interface PublicationService {

	<T extends Publishable> T resolvePublishable(Long mogulId, Serializable id, Class<T> clazz);

	/**
	 * given a request from a {@link com.joshlong.mogul.api.mogul.Mogul mogul}, is the
	 * plugin able to support publication
	 */
	<T extends Publishable> boolean canPublish(Long mogulId, Serializable serializable, Class<T> clazz,
			Map<String, String> contextAndSettings, PublisherPlugin<T> plugin);

	Publication getPublicationById(Long id);

	Collection<Publication> getPublicationsByPublicationKeyAndClass(Serializable publicationKey, Class<?> clazz);

	<T extends Publishable> Publication publish(Long mogulId, T payload, Map<String, String> contextAndSettings,
			PublisherPlugin<T> plugin);

	<T extends Publishable> Publication unpublish(Long mogulId, Publication publication, PublisherPlugin<T> plugin);

}
