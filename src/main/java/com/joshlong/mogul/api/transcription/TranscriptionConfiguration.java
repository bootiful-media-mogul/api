package com.joshlong.mogul.api.transcription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.openai.OpenAiAudioTranscriptionModel;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannelSpec;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;

import static com.joshlong.mogul.api.transcription.TranscriptionIntegrationConfiguration.TRANSCRIPTION_REPLIES_CHANNEL;
import static com.joshlong.mogul.api.transcription.TranscriptionIntegrationConfiguration.TRANSCRIPTION_REQUESTS_CHANNEL;

// todo figure out failure modes?
// - what if the reply comes back and the segment has been deleted?
// - what if there's an error in transcription? retry? durable requests?
// - how may requests can we handle at the same time? what's concurrence lok like? is this a job for `JobScheduler`?

@Configuration
@EnableConfigurationProperties(TranscriptionProperties.class)
class TranscriptionConfiguration {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final MessageChannel requests, replies;

	TranscriptionConfiguration(@Qualifier(TRANSCRIPTION_REQUESTS_CHANNEL) MessageChannel requests,
			@Qualifier(TRANSCRIPTION_REPLIES_CHANNEL) MessageChannel replies) {
		this.replies = replies;
		this.requests = requests;
	}

	// spring integration can do this.
	// we need to change this integration flow to do scatter-gather division of the file
	// and then route each tranch
	// through the transcription service then return a string and an X/N part and use that
	// to assemble a full transcript
	// that then gets set on the episode in aggregate.

	// todo rewrite this so that all requests are first delivered
	// to RabbitMQ so that they get redelivered if something should go wrong.

	@Bean
	IntegrationFlow transcriptionRequestsIntegrationFlow(TranscriptionService transcriptionService) {
		return IntegrationFlow//
			.from(this.requests)//
			.transform((GenericTransformer<TranscriptionRequest, TranscriptionReply>) payload -> {
				this.log.info("got a transcription request for [{}]", payload);
				var transcribable = payload.transcribable();
				var resource = transcribable.audio();
				var subject = transcribable.subject();
				var key = transcribable.transcriptionKey();
				try {
					var transcriptionReply = transcriptionService.transcribe(resource);
					return new TranscriptionReply(key, subject, transcriptionReply, null);
				} //
				catch (Throwable throwable) {
					this.log.error("got an exception when trying to provide a transcript {}", throwable.getMessage(),
							throwable);
					return new TranscriptionReply(key, subject, null,
							new TranscriptionReply.Error(throwable.getMessage()));
				}
			})//
			.channel(this.replies)
			.get();
	}

	@Bean
	IntegrationFlow transcriptionRepliesIntegrationFlow(ApplicationEventPublisher publisher) {
		return IntegrationFlow //
			.from(this.replies) //
			.handle((GenericHandler<TranscriptionReply>) (payload, headers) -> { //
				// todo use the spring integration event adapter?
				var transcriptionProcessedEvent = new TranscriptionProcessedEvent(payload.key(), payload.transcript(),
						payload.subject());
				publisher.publishEvent(transcriptionProcessedEvent);
				return null;
			})
			.get();
	}

	@Bean
	ChunkingTranscriptionService chunkingTranscriptionService(TranscriptionProperties transcriptionProperties,
			OpenAiAudioTranscriptionModel transcriptionModel) {
		return new ChunkingTranscriptionService(transcriptionModel, transcriptionProperties.root(), (10 * 1024 * 1024));
	}

	@Bean
	DefaultTranscriptionClient defaultTranscriptionClient() {
		return new DefaultTranscriptionClient(requests);
	}

}

@Configuration
class TranscriptionIntegrationConfiguration {

	static final String TRANSCRIPTION_REQUESTS_CHANNEL = "transcriptionRequestsMessageChannel";

	static final String TRANSCRIPTION_REPLIES_CHANNEL = "transcriptionRepliesMessageChannel";

	@Bean(name = TRANSCRIPTION_REPLIES_CHANNEL)
	MessageChannelSpec<DirectChannelSpec, DirectChannel> transcriptionRepliesMessageChannel() {
		return MessageChannels.direct();
	}

	@Bean(name = TRANSCRIPTION_REQUESTS_CHANNEL)
	MessageChannelSpec<DirectChannelSpec, DirectChannel> transcriptionRequestsMessageChannel() {
		return MessageChannels.direct();
	}

}