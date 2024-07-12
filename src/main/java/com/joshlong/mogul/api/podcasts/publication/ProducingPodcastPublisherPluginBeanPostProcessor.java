package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.podcasts.Episode;
import com.joshlong.mogul.api.podcasts.production.PodcastProducer;
import org.aopalliance.intercept.MethodInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactoryBean;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * introduces some behavior to produce - to glue together the intro, the bumper, and the
 * interview for - the audio for a given episode if and only if it hasn't already been
 * produced. this production happens lazily, just before a publication plugin is run
 */
class ProducingPodcastPublisherPluginBeanPostProcessor implements BeanPostProcessor {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final BeanFactory beanFactory;

	ProducingPodcastPublisherPluginBeanPostProcessor(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (bean instanceof PodcastEpisodePublisherPlugin plugin) {
			var proxyFactoryBean = new ProxyFactoryBean();
			proxyFactoryBean.addAdvice((MethodInterceptor) invocation -> {
				var podcastProducer = this.beanFactory.getBean(PodcastProducer.class);
				var podcastService = this.beanFactory.getBean(PodcastService.class);
				var publishMethod = invocation.getMethod().getName().equalsIgnoreCase("publish");
				if (publishMethod) {
					var context = (Map<String, String>) invocation.getArguments()[0];
					var episode = (Episode) invocation.getArguments()[1];
					var shouldProduceAudio = episode.producedAudioUpdated() == null
							|| episode.producedAudioUpdated().before(episode.producedAudioAssetsUpdated());
					log.debug("should produce the audio for episode [{}] from scratch? [{}]", episode,
							shouldProduceAudio);
					if (shouldProduceAudio) {
						var producedManagedFile = podcastProducer.produce(episode);
						log.debug(
								"produced the audio for episode [{}] from scratch to managedFile: [{}] using producer [{}]",
								episode, producedManagedFile, podcastProducer);
					}
					var updatedEpisode = podcastService.getEpisodeById(episode.id());
					Assert.notNull(updatedEpisode.producedAudioUpdated(), "the producedAudioUpdated field is null");
					plugin.publish(context, updatedEpisode);
					return null;
				}
				return invocation.proceed();
			});
			var targetClass = plugin.getClass();
			proxyFactoryBean.setInterfaces(targetClass.getInterfaces());
			proxyFactoryBean.setTargetClass(targetClass);
			proxyFactoryBean.setTarget(plugin);
			return proxyFactoryBean.getObject();
		}
		return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
	}

}
