package com.joshlong.mogul.api.media;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Map;

/**
 * the waveform of {@code in} has been drawn into {@code out}, or {@code success} is false
 * and {@code error} says why it couldn't be.
 */
public record AudioWaveformComputedEvent(ManagedFile in, ManagedFile out, boolean success, String error,
		Map<String, Object> context) {

}
