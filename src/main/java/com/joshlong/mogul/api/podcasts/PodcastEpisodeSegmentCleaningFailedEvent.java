package com.joshlong.mogul.api.podcasts;

/**
 * the segment couldn't be cleaned; its produced audio is rendered from the upload.
 * {@code actionRequired} means it won't work until the mogul fixes something.
 */
public record PodcastEpisodeSegmentCleaningFailedEvent(Long mogulId, Long episodeId, Long segmentId, String reason,
		boolean actionRequired) {

}
