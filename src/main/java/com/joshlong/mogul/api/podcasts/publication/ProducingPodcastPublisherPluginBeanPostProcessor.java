package com.joshlong.mogul.api.podcasts.publication;

import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import com.joshlong.mogul.api.podcasts.Episode;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.podcasts.production.PodcastProducer;
import org.aopalliance.intercept.MethodInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactoryBean;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.lang.NonNullApi;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * introduces some behavior to produce - to glue together the intro, the bumper, and the
 * interview for - the audio for a given episode if and only if it hasn't already been
 * produced. this production happens lazily, just before a publication plugin is run
 */
class ProducingPodcastPublisherPluginBeanPostProcessor implements BeanFactoryAware, BeanPostProcessor {

	private final AtomicReference<BeanFactory> beanFactoryAtomicReference = new AtomicReference<>();

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactoryAtomicReference.set(beanFactory);
		log.debug("obtained reference to BeanFactory in {}", getClass().getName());
	}

	public record PodcastEpisodeRenderStartedEvent(long episodeId) {

	}

	public record PodcastEpisodeRenderFinishedEvent(long episodeId) {

	}

	@Override
	@SuppressWarnings("unchecked")
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (bean instanceof PodcastEpisodePublisherPlugin plugin) {
			this.log.debug("bean {} is an instance of {}", beanName, PodcastEpisodePublisherPlugin.class.getName());
			var proxyFactoryBean = new ProxyFactoryBean();
			proxyFactoryBean.addAdvice((MethodInterceptor) invocation -> {
				var publishMethod = invocation.getMethod().getName().equalsIgnoreCase("publish");
				if (publishMethod) {

					var beanFactory = beanFactoryAtomicReference.get();

					var podcastProducer = beanFactory.getBean(PodcastProducer.class);
					var podcastService = beanFactory.getBean(PodcastService.class);
					var transactionTemplate = beanFactory.getBean(TransactionTemplate.class);

					this.log.debug("found the publish method on the plugin bean named {}", beanName);
					var context = (Map<String, String>) invocation.getArguments()[0];
					var episode = (Episode) invocation.getArguments()[1];

					var shouldProduceAudio = episode.producedAudioUpdated() == null
							|| episode.producedAudioUpdated().before(episode.producedAudioAssetsUpdated());
					this.log.debug("should produce the audio for episode [{}] from scratch? [{}]",
							"#" + episode.id() + " / " + episode.title(), shouldProduceAudio);
					var mogulId = podcastService.getPodcastById(episode.podcastId()).mogulId();
					return transactionTemplate.execute(status -> {

						if (shouldProduceAudio) {
							NotificationEvents.notifyAsync(NotificationEvent.notificationEventFor(mogulId,
									new PodcastEpisodeRenderStartedEvent(episode.id()), Long.toString(episode.id()),
									null, true, true));
							var producedManagedFile = podcastProducer.produce(episode);
							this.log.debug(
									"produced the audio for episode [{}] from scratch to managedFile: [{}] using producer [{}]",
									episode, producedManagedFile, podcastProducer);
							NotificationEvents.notifyAsync(NotificationEvent.notificationEventFor(mogulId,
									new PodcastEpisodeRenderFinishedEvent(episode.id()), Long.toString(episode.id()),
									null, true, true));
						}
						var updatedEpisode = podcastService.getPodcastEpisodeById(episode.id());
						Assert.notNull(updatedEpisode.producedAudioUpdated(), "the producedAudioUpdated field is null");
						plugin.publish(context, updatedEpisode);
						return null;
					});

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
