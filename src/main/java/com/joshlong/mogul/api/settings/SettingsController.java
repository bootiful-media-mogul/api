package com.joshlong.mogul.api.settings;

import com.joshlong.mogul.api.Settings;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.PublisherPlugin;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.QueryMapping;
import org.springframework.stereotype.Controller;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

	SettingsController(Map<String, PublisherPlugin<?>> ps, MogulService mogulService, Settings settings) {
		this.mogulService = mogulService;
		this.settings = settings;
		this.plugins.putAll(ps);
	}

	record Setting(String name, boolean valid, String value) {
	}

	record SettingsPage(boolean valid, String category, List<Setting> settings) {
	}

	@MutationMapping
	boolean updateSetting(@Argument String category, @Argument String name, @Argument String value) {
		this.settings.set(this.mogulService.getCurrentMogul().id(), category, name, value);
		return true;
	}

	@QueryMapping
	List<SettingsPage> settings() {

		var currentMogulId = this.mogulService.getCurrentMogul().id();
		var pages = new ArrayList<SettingsPage>();

		var orderedPluginNames = new ArrayList<>(this.plugins.keySet());
		orderedPluginNames.sort(Comparator.naturalOrder());

		for (var pluginName : orderedPluginNames) {
			var plugin = this.plugins.get(pluginName);
			var valuesByCategory = this.settings.getAllValuesByCategory(currentMogulId, pluginName);
			var pageIsValid = plugin.isConfigurationValid(valuesByCategory);
			var page = new SettingsPage(pageIsValid, pluginName, new ArrayList<>());
			pages.add(page);
			var requiredKeys = new ArrayList<>(plugin.getRequiredSettingKeys());
			requiredKeys.sort(Comparator.naturalOrder());
			for (var requiredKey : requiredKeys) {
				var value = settings.getValue(currentMogulId, pluginName, requiredKey);
				var valid = StringUtils.hasText(value);
				var setting = new Setting(requiredKey, valid, value);
				page.settings().add(setting);
			}
		}
		return pages;
	}

}
