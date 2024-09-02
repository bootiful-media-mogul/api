package com.joshlong.mogul.api.podcasts.publication;

import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;

@Configuration
@ImportRuntimeHints(PublicationConfiguration.Hints.class)
class PublicationConfiguration {

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			hints.proxies()
				.registerJdkProxy(PodcastEpisodePublisherPlugin.class, org.springframework.aop.SpringProxy.class,
						org.springframework.aop.framework.Advised.class,
						org.springframework.core.DecoratingProxy.class);

		}

	}

}
