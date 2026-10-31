package com.joshlong.mogul.api.settings;

import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.notifications.NotificationEvent;
import com.joshlong.mogul.api.notifications.NotificationEvents;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * the idea is that this endpoint should handle receiving updates to and displaying the
 * validity of all the various configuration values required by the system for each given
 * user.
 */
@Controller
class SettingsController {

	private final Map<String, PublisherPlugin<?>> plugins = new ConcurrentHashMap<>();

	private final MogulService mogulService;

	private final Settings settings;

	private final ObjectProvider<SettingsCategory> categories;

	SettingsController(Map<String, PublisherPlugin<?>> ps, ObjectProvider<SettingsCategory> categories,
			MogulService mogulService, Settings settings) {
		this.mogulService = mogulService;
		this.settings = settings;
		this.categories = categories;
		this.plugins.putAll(ps);
	}

	@MutationMapping
	boolean updateSetting(@Argument String category, @Argument String name, @Argument String value) {
		this.settings.set(this.mogulService.getCurrentMogul().id(), category, name, value);
		return true;
	}

	@ApplicationModuleListener
	void written(SettingWrittenEvent settingWrittenEvent) {
		var object = new SettingWrittenEvent(settingWrittenEvent.mogulId(), settingWrittenEvent.category(),
				settingWrittenEvent.key(), null); // remove the sensitive value.
		NotificationEvents.notify(NotificationEvent.visibleNotificationEventFor(settingWrittenEvent.mogulId(), object,
				settingWrittenEvent.key(), settingWrittenEvent.category() + '/' + settingWrittenEvent.key()));
	}

	@QueryMapping
	List<SettingsPage> settings() {
		var currentMogulId = this.mogulService.getCurrentMogul().id();
		// a publisher plugin's settings are all secrets, and are valid by its own rules;
		// a
		// category's are typed, and valid when everything it requires has a value
		var definitions = new HashMap<String, Set<SettingDefinition>>();
		for (var entry : this.plugins.entrySet())
			definitions.put(entry.getKey(),
					entry.getValue()
						.pluginSettings()
						.stream()
						.map(ps -> SettingDefinition.secret(ps.name(), ps.required()))
						.collect(Collectors.toSet()));
		for (var category : this.categories.orderedStream().toList())
			definitions.put(category.category(), category.settings());
		var pages = new ArrayList<SettingsPage>();
		var orderedCategories = new ArrayList<>(definitions.keySet());
		orderedCategories.sort(Comparator.naturalOrder());
		for (var category : orderedCategories) {
			var values = this.settings.getAllValuesByCategory(currentMogulId, category);
			var plugin = this.plugins.get(category);
			var settingsOnPage = definitions.get(category).stream().map(definition -> {
				var value = values.get(definition.name());
				return new Setting(definition.name(), StringUtils.hasText(value), value, definition.required(),
						definition.type().name(), definition.defaultValue());
			}).sorted(Comparator.comparing(Setting::name)).toList();
			var valid = plugin != null ? plugin.isConfigurationValid(values)
					: settingsOnPage.stream().allMatch(setting -> !setting.required() || setting.valid());
			pages.add(new SettingsPage(valid, category, new ArrayList<>(settingsOnPage)));
		}
		return pages;
	}

	record Setting(String name, boolean valid, String value, boolean required, String type, String defaultValue) {
	}

	record SettingsPage(boolean valid, String category, List<Setting> settings) {
	}

}
