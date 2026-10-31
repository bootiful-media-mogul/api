package com.joshlong.mogul.api.podcasts;

/**
 * the automatic cuts a mogul has turned on for a segment.
 *
 * @param fillers shorten each "um" cleanvoice found to a natural pause
 * @param stutters shorten each stutter the same way
 * @param silences cut long silences down, leaving a short pause
 * @param silenceLongerThan how long a silence has to be, in milliseconds, to be cut
 * @param silenceLeave how much of a cut silence to keep, in milliseconds
 */
public record SegmentFilters(boolean fillers, boolean stutters, boolean silences, long silenceLongerThan,
		long silenceLeave) {

	public static final long DEFAULT_SILENCE_LONGER_THAN = 2000;

	public static final long DEFAULT_SILENCE_LEAVE = 400;

	/** what a mogul gets when they've asked for the default filters */
	public static final SegmentFilters ON = new SegmentFilters(true, true, true, DEFAULT_SILENCE_LONGER_THAN,
			DEFAULT_SILENCE_LEAVE);

	public static final SegmentFilters OFF = new SegmentFilters(false, false, false, DEFAULT_SILENCE_LONGER_THAN,
			DEFAULT_SILENCE_LEAVE);

}
