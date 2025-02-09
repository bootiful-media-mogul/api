package com.joshlong.mogul.api;

import java.io.Serializable;

/**
 * we need a way to, given a {@link Serializable id} and a {@link Class class}, find the
 * given instance of the {@link Publishable publishable}. The logic for that will differ
 * based on the subsystem; e.g.: the {@link com.joshlong.mogul.api.blogs.BlogService
 * blogService} can resolve {@link com.joshlong.mogul.api.blogs.Post posts}, and the
 * {@link com.joshlong.mogul.api.podcasts.PodcastService podcastService} can resolve
 * {@link com.joshlong.mogul.api.podcasts.Episode episodes}. So this class helps provide a
 * uniform strategy for loading a {@link Publishable publishable}.
 *
 * @param <T>
 */
public interface PublishableRepository<T extends Publishable> {

	boolean supports(Class<?> clazz);

	T find(Serializable serializable);

}
