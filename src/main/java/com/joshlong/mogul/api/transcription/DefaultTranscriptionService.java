package com.joshlong.mogul.api.transcription;

import com.joshlong.mogul.api.Transcribable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
class DefaultTranscriptionService implements TranscriptionService {

	private final MessageChannel requests;

	DefaultTranscriptionService(
			@Qualifier(TranscriptionConfiguration.TRANSCRIPTION_REQUESTS_CHANNEL) MessageChannel requests) {
		this.requests = requests;
	}

	@Override
	public void requestTranscription(Transcribable transcribable) throws Exception {
		var message = MessageBuilder.withPayload(new TranscriptionRequest(transcribable)).build();
		this.requests.send(message);
	}

}
