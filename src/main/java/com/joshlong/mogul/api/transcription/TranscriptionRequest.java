package com.joshlong.mogul.api.transcription;

/**
 * contains a pointer to the thing that supports transcription (typically bytes backed 
 * by a {@link  com.joshlong.mogul.api.managedfiles.ManagedFile managed file}) for an audio file.
 * 
 * @param transcribable
 */
public record TranscriptionRequest(  Transcribable transcribable  )  {
}
