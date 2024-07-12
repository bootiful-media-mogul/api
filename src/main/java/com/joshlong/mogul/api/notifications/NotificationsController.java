package com.joshlong.mogul.api.notifications;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.mogul.Mogul;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Controller
class NotificationsController {

	private final Logger log = LoggerFactory.getLogger(getClass());

	/**
	 * mapping between {@link Mogul mogul} ID and a given SSE emitter
	 */
	private final Map<Long, SseEmitter> sseSessions = new ConcurrentHashMap<>();

	private final ObjectMapper objectMapper;

	private final MogulService mogulService;

	NotificationsController(ObjectMapper objectMapper, MogulService mogulService) {
		this.objectMapper = objectMapper;
		this.mogulService = mogulService;
	}

	private static void deliver(ObjectMapper om, SseEmitter emitter, NotificationEvent event) {
		try {
			var json = om.writeValueAsString(event);
			emitter.send(json, MediaType.APPLICATION_JSON);
		} //
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@ApplicationModuleListener
	void notificationEventListener(NotificationEvent notification) {
		Assert.notNull(notification, "the notification must not be null");
		var mogulId = notification.mogulId();

		for (var loggedInMogulId : this.sseSessions.keySet()) {
			var deliverToThisMogul = notification.mogulId() == null || (loggedInMogulId.equals(notification.mogulId()));
			if (deliverToThisMogul) {
				log.debug("got a notification for this Mogul # {}::{}", mogulId, notification);
				var sse = this.sseSessions.get(loggedInMogulId);
				deliver(this.objectMapper, sse, notification);
			}
		}

	}

	@GetMapping("/notifications")
	SseEmitter sseEmitter() {
		var currentMogulId = this.mogulService.getCurrentMogul().id();
		return this.sseSessions.computeIfAbsent(currentMogulId, mogul -> {
			var runnable = (Runnable) () -> this.sseSessions.remove(currentMogulId);
			var sse = new SseEmitter();
			sse.onTimeout(runnable);
			sse.onCompletion(runnable);
			return sse;
		});
	}

}
