package com.joshlong.mogul.api.media;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Map;

/**
 * the normalization of {@code in} into {@code out} failed. the segment editor waits on a
 * render after a save; without this it would wait forever.
 */
public record MediaNormalizationFailedEvent(ManagedFile in, ManagedFile out, String error,
		Map<String, Object> context) {

}
