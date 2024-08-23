package com.joshlong.mogul.api.notifications;

import com.joshlong.mogul.api.mogul.MogulService;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Controller
@RegisterReflectionForBinding(NotificationEvent.class)
class NotificationsController {

	private final Map<Long, Queue<NotificationEvent>> events = new ConcurrentHashMap<>();

	private final MogulService mogulService;

	NotificationsController(MogulService mogulService) {
		this.mogulService = mogulService;
	}

	@ApplicationModuleListener
	void notificationEventListener(NotificationEvent notification) {
		Assert.notNull(notification, "the notification must not be null");
		var mogulId = notification.mogulId();
		this.events.computeIfAbsent(mogulId, aLong -> new ConcurrentLinkedQueue<>()).add(notification);
	}

	@QueryMapping
	Map<String, Object> notifications() {
		var currentMogulId = this.mogulService.getCurrentMogul().id();
		var notificationEvents = this.events.computeIfAbsent(currentMogulId, mogulId -> new ConcurrentLinkedQueue<>());
		var notification = notificationEvents.poll();
		if (null != notification) {
			Assert.notNull(currentMogulId, "the current mogul id should not be null");
			Assert.state(notification.when().getTime() > 0, "the time must not be null");
			Assert.state(StringUtils.hasText(notification.context()), "the context must not be null");
			Assert.state(StringUtils.hasText(notification.key()), "the key must not be null");
			Assert.state(StringUtils.hasText(notification.category()), "the category must not be null");
			return Map.of("mogulId", currentMogulId, //
					"when", notification.when().getTime(), //
					"context", notification.context(), "key", notification.key(), "category", notification.category(),
					"modal", notification.modal());
		}
		return null;
	}

}
