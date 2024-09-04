package com.joshlong.mogul.api.ai;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.image.ImageMessage;
import org.springframework.ai.image.ImageModel;
import org.springframework.ai.image.ImageOptionsBuilder;
import org.springframework.ai.image.ImagePrompt;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import java.net.MalformedURLException;

class DefaultAiClient implements AiClient {

	private final ImageModel imageModel;

	private final ChatClient aiClient;

	DefaultAiClient(ImageModel imageModel, ChatClient aiClient) {
		this.imageModel = imageModel;
		this.aiClient = aiClient;
	}

	@Override
	public String chat(String prompt) {
		return this.aiClient.prompt().user(prompt).call().content();
	}

	@Override
	public Resource render(String prompt, ImageSize imageSize) {
		try {
			var imageOptions = ImageOptionsBuilder.builder()
				.withWidth(imageSize.width())
				.withWidth(imageSize.height())
				.build();
			var imageResponse = this.imageModel.call(new ImagePrompt(new ImageMessage(prompt), imageOptions));
			return new UrlResource(imageResponse.getResult().getOutput().getUrl());
		} //
		catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

}
