package com.joshlong.mogul.api.ai;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.image.ImageMessage;
import org.springframework.ai.image.ImageModel;
import org.springframework.ai.image.ImageOptionsBuilder;
import org.springframework.ai.image.ImagePrompt;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.StringUtils;

import java.net.MalformedURLException;
import java.util.Locale;
import java.util.Objects;

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
	public String chat(String prompt, String language) {
		if (!StringUtils.hasText(language))
			return this.chat(prompt);
		var name = Locale.forLanguageTag(language).getDisplayLanguage(Locale.ENGLISH);
		var instruction = "Write your entire answer in " + (StringUtils.hasText(name) ? name : language)
				+ ", the language the user's system is set to.";
		return this.aiClient.prompt().system(instruction).user(prompt).call().content();
	}

	@Override
	public Resource render(String prompt, ImageSize imageSize) {
		try {
			var imageOptions = ImageOptionsBuilder.builder()
				.width(imageSize.width())
				.height(imageSize.height())
				.build();
			var imageResponse = this.imageModel.call(new ImagePrompt(new ImageMessage(prompt), imageOptions));
			return new UrlResource(Objects.requireNonNull(Objects.requireNonNull(imageResponse.getResult())//
				.getOutput()
				.getUrl()));
		} //
		catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

}
