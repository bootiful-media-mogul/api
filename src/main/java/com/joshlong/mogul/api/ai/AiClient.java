package com.joshlong.mogul.api.ai;

import org.springframework.core.io.Resource;

public interface AiClient {

	String chat(String prompt);

	/**
	 * asks for the answer in the language given as a BCP 47 tag ({@code es},
	 * {@code fr-CA}). a null or blank language leaves the model's default in place.
	 */
	String chat(String prompt, String language);

	Resource render(String prompt, ImageSize imageSize);

}
