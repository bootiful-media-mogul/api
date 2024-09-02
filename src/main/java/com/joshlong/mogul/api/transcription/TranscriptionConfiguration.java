package com.joshlong.mogul.api.transcription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.openai.OpenAiAudioTranscriptionModel;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannelSpec;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;

import java.io.*;
import java.util.List;

// todo figure out failure modes?
// - what if the reply comes back and the segment has been deleted?
// - what if there's an error in transcription? retry? durable requests?
// - how may requests can we handle at the same time? what's concurrence lok like? is this a job for `JobScheduler`?

@Configuration
class TranscriptionConfiguration {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private static final String TRANSCRIPTION_REQUESTS_CHANNEL = "transcriptionRequestsMessageChannel";

	private static final String TRANSCRIPTION_REPLIES_CHANNEL = "transcriptionRepliesMessageChannel";

	@Bean
	DefaultTranscriptionService transcriptionService(
			@Qualifier(TRANSCRIPTION_REQUESTS_CHANNEL) MessageChannel requests) {
		return new DefaultTranscriptionService(requests);
	}

	@Bean(name = TRANSCRIPTION_REPLIES_CHANNEL)
	MessageChannelSpec<DirectChannelSpec, DirectChannel> transcriptionReplies() {
		return MessageChannels.direct();
	}

	@Bean(name = TRANSCRIPTION_REQUESTS_CHANNEL)
	MessageChannelSpec<DirectChannelSpec, DirectChannel> transcriptionRequests() {
		return MessageChannels.direct();
	}

	// spring integration can do this.
	// we need to change this integration flow to do scatter-gather division of the file
	// and then route each tranch
	// through the transcription service then return a string and an X/N part and use that
	// to assemble a full transcript
	// that then gets set on the episode in aggregate.

	@Bean
	IntegrationFlow transcriptionRequestsIntegrationFlow(
			@Qualifier(TRANSCRIPTION_REQUESTS_CHANNEL) MessageChannel requests,
			@Qualifier(TRANSCRIPTION_REPLIES_CHANNEL) MessageChannel replies,
			OpenAiAudioTranscriptionModel openAiAudioTranscriptionModel) {
		return IntegrationFlow//
			.from(requests)//
			.transform((GenericHandler<TranscriptionRequest>) (payload, headers) -> {
				this.log.debug("got a transcription request for [{}]", payload);
				var transcribable = payload.transcribable();
				var resource = transcribable.audio();
				var subject = transcribable.subject();
				var key = transcribable.transcriptionKey();
				try {
					this.log.debug("about to perform transcription using OpenAI for transcribable with key {}", key);
					var transcriptionReply = openAiAudioTranscriptionModel.call(resource);
					return new TranscriptionReply(key, subject, transcriptionReply, null);
				} //
				catch (Throwable throwable) {
					return new TranscriptionReply(key, subject, null,
							new TranscriptionReply.Error(throwable.getMessage()));
				}
			})//
			.channel(replies)
			.get();
	}

	@Bean
	IntegrationFlow transcriptionRepliesIntegrationFlow(ApplicationEventPublisher publisher,
			@Qualifier(TRANSCRIPTION_REPLIES_CHANNEL) MessageChannel replies) {
		return IntegrationFlow //
			.from(replies) //
			.handle((GenericHandler<TranscriptionReply>) (payload, headers) -> { //
				// todo use the spring integration event adapter?
				var transcriptionProcessedEvent = new TranscriptionProcessedEvent(payload.key(), payload.transcript(),
						payload.subject());
				publisher.publishEvent(transcriptionProcessedEvent);
				return null;
			})
			.get();
	}

}

record TranscriptionSegment(Resource audio, int order, String start, String stop) {
}

record TranscriptionBatch(List<TranscriptionSegment> segments) {
}

record TranscriptionReply(Serializable key, Class<?> subject, String transcript, Error error) {

	record Error(String details) {
	}

}