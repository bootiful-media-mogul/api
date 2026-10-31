package com.joshlong.mogul.api.podcasts;

/**
 * the segment's audio has been cleaned, and its produced audio rendered from the result.
 */
public record PodcastEpisodeSegmentCleanedEvent(Long mogulId, Long episodeId, Long segmentId) {

}
