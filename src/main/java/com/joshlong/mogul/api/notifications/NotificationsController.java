package com.joshlong.mogul.api.notifications;

import com.joshlong.mogul.api.mogul.MogulService;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.Objects;
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
		var queue = this.events.computeIfAbsent(mogulId, aLong -> new ConcurrentLinkedQueue<>());
		queue.add(notification);
	}

	@QueryMapping
	Map<String, Object> notifications() {
		var currentMogulId = this.mogulService.getCurrentMogul().id();
		var notificationEvents = this.events.getOrDefault(currentMogulId, new ConcurrentLinkedQueue<>());
		var notification = notificationEvents.poll();
		if (null != notification)
			return Map.of("mogulId", Objects.requireNonNull(notification).mogulId(), //
					"when", notification.when().getTime(), //
					"key", notification.key(), "category", notification.category(), "modal", notification.modal());
		return null;
	}

}
