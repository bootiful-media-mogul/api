package com.joshlong.mogul.api.podcasts.publication;

import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;

@Configuration
@ImportRuntimeHints(PodcastEpisodePublisherPluginConfiguration.Hints.class)
class PodcastEpisodePublisherPluginConfiguration {

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			hints.proxies()
				.registerJdkProxy(PodcastEpisodePublisherPlugin.class,
						org.springframework.beans.factory.BeanNameAware.class,
						org.springframework.aop.SpringProxy.class, org.springframework.aop.framework.Advised.class,
						org.springframework.core.DecoratingProxy.class);
		}

	}

	@Bean
	static ProducingPodcastPublisherPluginBeanPostProcessor podcastProducingBeanPostProcessor(BeanFactory beanFactory) {
		return new ProducingPodcastPublisherPluginBeanPostProcessor();
	}

}
