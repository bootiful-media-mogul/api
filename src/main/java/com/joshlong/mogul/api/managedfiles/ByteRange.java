package com.joshlong.mogul.api.managedfiles;

import org.springframework.http.HttpRange;

/**
 * the one range of a file a {@code Range} request asks for, both ends inclusive, as HTTP
 * counts them. browsers ask for ranges to stream and seek in audio and video: without
 * answers, Chrome can't seek and Safari won't play at all.
 * <p>
 * Spring answers range requests for most {@link org.springframework.core.io.Resource}s by
 * itself, but not for the streams that come back from S3, which is why this is here.
 */
record ByteRange(long start, long end, long size) {

	/** what a {@code Range} request gets when it asks for bytes the file doesn't have */
	static final ByteRange UNSATISFIABLE = new ByteRange(-1, -1, -1);

	/**
	 * the range to send, {@link #UNSATISFIABLE}, or null to send the whole file. a
	 * malformed header and a request for several ranges at once get the whole file, which
	 * HTTP allows: a server may always ignore {@code Range}.
	 */
	static ByteRange of(String rangeHeader, long size) {
		if (rangeHeader == null || rangeHeader.isBlank()) {
			return null;
		}
		try {
			var ranges = HttpRange.parseRanges(rangeHeader);
			if (ranges.size() != 1) {
				return null;
			}
			var range = ranges.getFirst();
			if (size <= 0) {
				return UNSATISFIABLE;
			}
			// HttpRange clamps the end to the file but doesn't check the start against it
			var start = range.getRangeStart(size);
			return start >= size ? UNSATISFIABLE : new ByteRange(start, range.getRangeEnd(size), size);
		} //
		catch (IllegalArgumentException e) {
			return null;
		}
	}

	long length() {
		return this.end - this.start + 1;
	}

	/** the {@code Content-Range} header for this range */
	String contentRange() {
		return "bytes " + this.start + "-" + this.end + "/" + this.size;
	}

}
