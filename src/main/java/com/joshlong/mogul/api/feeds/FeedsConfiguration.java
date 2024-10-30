package com.joshlong.mogul.api.feeds;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;

//  hmm. do i want this to be conditional on me actually using the {@link FeedTemplate}? Not sure.

@Configuration
@ImportRuntimeHints(FeedRuntimeHintsRegistrar.class)
class FeedsConfiguration {

	@Bean
	FeedTemplate feedTemplate() {
		return new FeedTemplate();
	}

}
