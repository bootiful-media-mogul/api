package com.joshlong.mogul.api.podcasts.publication;

/**
 * a publication of the episode is waiting for its segments to finish being cleaned.
 */
public record PodcastEpisodePublicationWaitingForCleaningEvent(Long mogulId, Long episodeId) {

}
