package com.joshlong.mogul.api.ai;

import com.joshlong.mogul.api.mogul.MogulService;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;

@Controller
class AiController {

	private final DefaultAiClient singularity;

	private final MogulService mogulService;

	AiController(DefaultAiClient singularity, MogulService mogulService) {
		Assert.notNull(singularity, "the AI client is null");
		this.singularity = singularity;
		this.mogulService = mogulService;
	}

	@QueryMapping
	String aiChat(@Argument String prompt) {
		return this.singularity.chat(prompt, this.mogulService.getCurrentMogul().language());
	}

}
