package com.joshlong.mogul.api.ai;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.image.ImageModel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;

@Configuration
@ImportRuntimeHints(OpenAiSdkHints.class)
class AiClientConfiguration {

	@Bean
	ChatClient chatClient(ChatClient.Builder builder) {
		return builder.build();
	}

	@Bean
	DefaultAiClient aiClient(ImageModel imageModel, ChatClient chatClient) {
		return new DefaultAiClient(imageModel, chatClient);
	}

}
