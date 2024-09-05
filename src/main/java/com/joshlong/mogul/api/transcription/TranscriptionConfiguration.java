package com.joshlong.mogul.api.transcription;

import org.springframework.ai.openai.OpenAiAudioTranscriptionModel;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(TranscriptionProperties.class)
class TranscriptionConfiguration {

	@Bean
	ChunkingTranscriber chunkingTranscriptionService(TranscriptionProperties transcriptionProperties,
			OpenAiAudioTranscriptionModel transcriptionModel) {
		return new ChunkingTranscriber(transcriptionModel, transcriptionProperties.root(), (10 * 1024 * 1024));
	}

}
