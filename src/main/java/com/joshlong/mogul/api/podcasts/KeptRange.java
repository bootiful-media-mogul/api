package com.joshlong.mogul.api.podcasts;

import org.springframework.util.Assert;

/**
 * one stretch of a segment's uploaded audio to keep, in milliseconds from its start. an
 * edited segment is the list of these, played back to back.
 *
 * @param start where the range starts, in milliseconds
 * @param end where the range ends, in milliseconds
 */
public record KeptRange(long start, long end) {

	public KeptRange {
		Assert.state(start >= 0, "a kept range can't start before the audio does");
		Assert.state(end > start, "a kept range must end after it starts");
	}

}
