package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Publication;
import com.joshlong.mogul.api.Publishable;
import com.joshlong.mogul.api.PublisherPlugin;

import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;

/**
 * handles preparing context, launching the {@link PublisherPlugin}, and noting the
 * publication in the DB.
 */
public interface PublicationService {

	String MOGUL_ID = "mogulId";

	String PUBLICATION_ID = "publicationId";

	/**
	 * the absolute base URL of the client/studio (e.g. {@code https://host}), captured
	 * from the gateway-supplied request header so plugins can build absolute links back
	 * into the app (e.g. to a newly created blog post). present in a plugin's
	 * {@code PublishContext.context()} only when the header was available.
	 */
	String BASE_URL = "baseUrl";

	/**
	 * the HTTP header the gateway stamps on requests with the client's absolute base URL.
	 */
	String BASE_URL_HEADER = "X-Mogul-Base-Url";

	/**
	 * the string-valued details of the authentication that asked for the publication
	 * (tokens the gateway propagated, like WordPress'), as json. the plugin runs later,
	 * on a jobrunr thread, with no request in sight; this is how it gets them back. the
	 * publication's context is encrypted at rest, like the settings already in it.
	 */
	String AUTHENTICATION_DETAILS = "authenticationDetails";

	<T extends Publishable> T resolvePublishable(Long mogulId, Long id, String clazz);

	<T extends Publishable> T resolvePublishable(Long mogulId, Long id, Class<T> clazz);

	Publication getPublicationById(Long id);

	Map<Long, Publication> getPublicationsByIds(Collection<Long> ids);

	Collection<Publication> getPublicationsByPublicationKeyAndClass(Long publicationKey, Class<?> clazz);

	Collection<Publication> getPublicationsByPublicationKeyAndClass(Long publicationKey, String clazz);

	Map<Long, Collection<Publication>> getPublicationsByPublicationKeysAndClass(Collection<Long> publicationKeys,
			Class<?> clazz);

	/**
	 * records a {@link Publication.State#PENDING pending} publication and hands it to
	 * jobrunr, which runs the plugin at {@code when} -- or as soon as it can, if
	 * {@code when} is {@code null} or already past. all roads lead to jobrunr: this
	 * returns before anything has been published, and the client learns how it went the
	 * same way whether it was for right now or for next week, from the notifications that
	 * follow.
	 * @param when the moment to publish, or {@code null} for as soon as possible
	 */
	<T extends Publishable> Publication publish(Long mogulId, T payload, Map<String, String> contextAndSettings,
			PublisherPlugin<T> plugin, @Nullable Instant when);

	/**
	 * runs the attempt's plugin and records how it went. jobrunr calls this, by way of
	 * {@link PublishRequestHandler}, once the publication's time has come and every
	 * {@link PublicationGate} has let it through.
	 */
	<T extends Publishable> Publication completePublication(PublicationAttempt<T> attempt);

	/**
	 * abandons an attempt that never got as far as its plugin, recording why. for the
	 * work that has to happen before a publication can run -- see {@link PublicationGate}
	 * -- and that didn't: publishing anyway would send whatever the last render left
	 * behind, and publishing nothing at all would leave the client waiting on a
	 * publication that is never going to move.
	 */
	<T extends Publishable> Publication failPublication(PublicationAttempt<T> attempt, String error);

	record PublicationAttempt<T extends Publishable>(Long publicationId, Long mogulId, PublisherPlugin<T> plugin,
			PublisherPlugin.PublishContext<T> publishContext) {
	}

	<T extends Publishable> Publication unpublish(Long mogulId, Publication publication, PublisherPlugin<T> plugin);

}
