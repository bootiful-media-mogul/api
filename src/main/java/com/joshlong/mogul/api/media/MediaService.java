package com.joshlong.mogul.api.media;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.List;
import java.util.Map;

/**
 * hands the expensive parts of dealing with media -- {@code ffmpeg}, {@code magick} -- to
 * the {@code processors} module. both of these return the moment the request is on the
 * wire; the work happens in another process and announces itself later with an event
 * carrying the {@code context} the caller sent.
 */
public interface MediaService {

	void normalize(ManagedFile input, ManagedFile output, Map<String, Object> context);

	/**
	 * like {@link #normalize(ManagedFile, ManagedFile, Map)}, but keeps only
	 * {@code keptRanges} of the input, back to back: {@code start-end} pairs in
	 * milliseconds separated by commas, {@code 0-30000,60000-234960}. empty keeps it all.
	 */
	void normalize(ManagedFile input, ManagedFile output, String keptRanges, Map<String, Object> context);

	/**
	 * draws the waveform of the audio in {@code input} into {@code output}. finishes with
	 * an {@link AudioWaveformComputedEvent}.
	 */
	void waveform(ManagedFile input, ManagedFile output, Map<String, Object> context);

	/**
	 * joins {@code inputs}, in order, into one mp3 written over {@code output}. finishes
	 * with an {@link AudioProducedEvent}.
	 */
	void produce(List<ManagedFile> inputs, ManagedFile output, Map<String, Object> context);

}
