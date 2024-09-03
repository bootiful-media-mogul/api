package com.joshlong.mogul.api.transcription;

import java.io.Serializable;

record TranscriptionReply(Serializable key, Class<?> subject, String transcript, Error error) {

	record Error(String details) {
	}

}
