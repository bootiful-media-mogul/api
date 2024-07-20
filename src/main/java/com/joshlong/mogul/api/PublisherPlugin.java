package com.joshlong.mogul.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public interface PublisherPlugin<T> {

	String name();

	Set<String> getRequiredSettingKeys();

	default boolean isConfigurationValid(Map<String, String> context) {
		var required = this.getRequiredSettingKeys();

		if (context == null)
			context = new HashMap<>();

		if (required == null)
			required = new HashSet<>();

		var good = true;
		for (var k : required) {
			if (!context.containsKey(k)) {
				good = false;
				break;
			}
		}
		return good;
	}

	default boolean canPublish(Map<String, String> context, T payload) {
		return isConfigurationValid(context) && payload != null;
	}

	void publish(Map<String, String> context, T payload);

	void unpublish(Map<String, String> context, T payload);

}
