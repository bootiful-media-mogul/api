package com.joshlong.mogul.api.notifications;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class NotificationEvents implements BeanFactoryAware, ApplicationEventPublisherAware {

	private final static Logger log = LoggerFactory.getLogger(NotificationEvents.class);

	private static final AtomicReference<ApplicationEventPublisher> PUBLISHER_ATOMIC_REFERENCE = new AtomicReference<>();

	private static final AtomicReference<BeanFactory> BEAN_FACTORY_ATOMIC_REFERENCE = new AtomicReference<>();

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		BEAN_FACTORY_ATOMIC_REFERENCE.set(beanFactory);
	}

	private static TransactionTemplate transactionTemplate() {
		return BEAN_FACTORY_ATOMIC_REFERENCE.get().getBean(TransactionTemplate.class);
	}


	private static final Executor EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

	private static Executor executor() {
		return EXECUTOR ;
	}

	private static ApplicationEventPublisher eventPublisher() {
		return PUBLISHER_ATOMIC_REFERENCE.get();
	}

	/**
	 * Do this on a separate thread so that it's not included in the longer running parent
	 * transaction.
	 * @param event the {@link NotificationEvent}
	 */
	public static void notifyAsync(NotificationEvent event) {
		var executor = executor();
		var transactionTemplate = transactionTemplate();
		Assert.notNull(executor, "the executor must not be null");
		Assert.notNull(transactionTemplate, "the transaction template must not be null");
		executor.execute(() -> transactionTemplate.executeWithoutResult(transactionStatus -> notify(event)));
	}

	/**
	 * Notify in the same calling thread, allowing it to bind to the current ongoing
	 * {@link org.springframework.transaction.support.TransactionSynchronization tx}
	 * @param event the {@link NotificationEvent}
	 */
	public static void notify(NotificationEvent event) {
		var eventPublisher = eventPublisher();
		Assert.notNull(eventPublisher, "the publisher must not be null");
		eventPublisher.publishEvent(event);
		if (log.isDebugEnabled()) {
			log.debug("published {} [{}]", NotificationEvent.class.getName(), event);
		}
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		PUBLISHER_ATOMIC_REFERENCE.set(applicationEventPublisher);
	}

}
