package com.joshlong.mogul.api.transcription;

import java.io.Serializable;

/**
 * published after a transcript has been returned. it's up to each interested party to
 * inspect the key and handle the event if it's interesting to them
 */
public record TranscriptionProcessedEvent(Serializable key, String transcript) {
}
