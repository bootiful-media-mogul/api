package com.joshlong.mogul.api.transcription;

import com.joshlong.mogul.api.Transcribable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

class DefaultTranscriptionClient implements TranscriptionClient {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final MessageChannel requests;

	DefaultTranscriptionClient(MessageChannel requests) {
		this.requests = requests;
	}

	@Override
	public void startTranscription(Transcribable transcribable) {
		var message = MessageBuilder.withPayload(new TranscriptionRequest(transcribable)).build();
		this.requests.send(message);
		this.log.debug("requesting transcription for transcribable with key {} and subject {}",
				transcribable.transcriptionKey().toString(), transcribable.subject().getName());
	}

}
