package com.joshlong.mogul.api.podcasts;

import com.joshlong.mogul.api.Searchable;
import com.joshlong.mogul.api.Transcribable;
import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.List;

/**
 * represents an arbitrary segment of a podcast episode.
 *
 * @param id the episode segment's ID
 * @param audio the raw source audio for a segment
 * @param producedAudio the produced audio for a segment: {@code audio} re-encoded, with
 * only the {@code edits} kept
 * @param waveform the waveform of {@code audio} the segment editor draws. null for
 * segments created before there were waveforms, until the editor asks for one
 * @param edits the ranges of {@code audio} to keep, in the order they play. null if the
 * segment hasn't been edited, which keeps all of it
 * @param crossFadeDuration the duration of the crossfade, if any. Default is 0.
 * @param name the name of the segment
 * @param order the relative order of the segment
 */
public record Segment(Long episodeId, Long id, ManagedFile audio, ManagedFile producedAudio, ManagedFile waveform,
		List<KeptRange> edits, long crossFadeDuration, String name, int order,
		long duration) implements Transcribable, Searchable {

	@Override
	public Long transcribableId() {
		return this.id();
	}

	@Override
	public Long searchableId() {
		return this.id();
	}

}