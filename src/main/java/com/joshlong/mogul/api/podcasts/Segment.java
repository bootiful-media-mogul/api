package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Searchable;
import com.joshlong.mogul.api.Transcribable;
import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * represents an arbitrary segment of a podcast episode.
 *
 * @param id the episode segment's ID
 * @param audio the raw source audio for a segment, exactly as uploaded
 * @param producedAudio what goes into the episode: the cleaned audio if there is any,
 * otherwise the upload, re-encoded, with only the kept ranges
 * @param waveform the waveform the segment editor draws. null for segments created before
 * there were waveforms, until the editor asks for one
 * @param cleanedAudio cleanvoice's cleaned copy of {@code audio}, or null
 * @param edits the ranges of {@code audio} to keep by hand, in the order they play. null
 * keeps all of it
 * @param filters the automatic cuts chosen for this segment, or null if none were
 * @param restored ids of detections put back despite the filters
 * @param detections json: what cleanvoice muted and where the long silences are
 * @param cleanedFromEtag the etag of the upload {@code cleanedAudio} was made from
 * @param cleaningStarted when the current cleaning started, or null if there isn't one
 * @param cleaningError why the last cleaning failed, or null
 * @param crossFadeDuration the duration of the crossfade, if any. Default is 0.
 * @param name the name of the segment
 * @param order the relative order of the segment
 */
public record Segment(Long episodeId, Long id, ManagedFile audio, ManagedFile producedAudio, ManagedFile waveform,
		ManagedFile cleanedAudio, List<KeptRange> edits, SegmentFilters filters, Set<String> restored,
		String detections, String cleanedFromEtag, Date cleaningStarted, String cleaningError, long crossFadeDuration,
		String name, int order, long duration) implements Transcribable, Searchable {

	/**
	 * whether there's a cleaned copy of the audio as it is now. an upload since the
	 * cleaning makes it stale, and it's the upload that counts.
	 */
	public boolean cleaned() {
		return this.cleanedAudio != null && this.cleanedAudio.written() && this.audio != null
				&& this.audio.etag() != null && this.audio.etag().equals(this.cleanedFromEtag);
	}

	public boolean cleaning() {
		return this.cleaningStarted != null;
	}

	@Override
	public Long transcribableId() {
		return this.id();
	}

	@Override
	public Long searchableId() {
		return this.id();
	}

}
