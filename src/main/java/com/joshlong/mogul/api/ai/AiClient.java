package com.joshlong.mogul.api.ai;

import org.springframework.core.io.Resource;

// todo could we fold the transcription stuff into this?
public interface AiClient {

	String chat(String prompt);

	Resource render(String prompt, ImageSize imageSize);

}
