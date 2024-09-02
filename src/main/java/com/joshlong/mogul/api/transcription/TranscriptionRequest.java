package com.joshlong.mogul.api.transcription;

import com.joshlong.mogul.api.Transcribable;

/**
 * contains a pointer to the thing that supports transcription (typically bytes backed by
 * a {@link com.joshlong.mogul.api.managedfiles.ManagedFile managed file}) for an audio
 * file.
 *
 * @param transcribable
 */
record TranscriptionRequest(Transcribable transcribable) {
}
