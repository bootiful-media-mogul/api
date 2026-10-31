package com.joshlong.mogul.api.media;

import com.joshlong.mogul.api.managedfiles.ManagedFile;

import java.util.Map;

/**
 * cleanvoice's cleaned audio has been fetched into {@code out}, and {@code detections} is
 * what was found in it -- or {@code success} is false and {@code error} says why not.
 */
public record CleanedAudioIngestedEvent(ManagedFile out, boolean success, String error, String detections,
		Map<String, Object> context) {

}
