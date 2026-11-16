package com.joshlong.mogul.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Repository interface for entities that can be published.
 * <p>
 * Provides a uniform strategy for loading {@link Publishable} instances across different
 * subsystems. The logic differs based on the subsystem; e.g.: the
 * {@link com.joshlong.mogul.api.blogs.BlogService blogService} can resolve
 * {@link com.joshlong.mogul.api.blogs.Post posts}, and the
 * {@link com.joshlong.mogul.api.podcasts.PodcastService podcastService} can resolve
 * {@link com.joshlong.mogul.api.podcasts.Episode episodes}.
 * <p>
 * Extends {@link DomainResolver} to follow the common domain pattern convention.
 *
 * @param <T> The concrete entity type that implements Publishable
 */
public interface PublishableResolver<T extends Publishable> extends DomainResolver<Publishable, T> {

	/**
	 * many at once, keyed by id, for the screens that show publications of every kind
	 * together. the default asks one at a time; resolvers that can should do it in one
	 * query. ids that don't resolve are left out.
	 */
	default Map<Long, T> findAll(Collection<Long> keys) {
		var map = new HashMap<Long, T>();
		for (var key : keys) {
			var found = this.find(key);
			if (found != null)
				map.put(key, found);
		}
		return map;
	}

	/**
	 * what to call the publishable where it's listed alongside other kinds of
	 * publishable, like the home page's calendar.
	 */
	String title(T publishable);

	/**
	 * the id of the mogul who owns this publishable, so that nobody can see or publish
	 * someone else's things.
	 */
	Long mogulId(T publishable);

}
