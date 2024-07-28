package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Settings;

import java.util.Map;
import java.util.function.Function;

// todo make this package private and maybe even move it back into the service where its used
//  after the migration is done
public class SettingsLookupClient implements Function<DefaultPublicationService.SettingsLookup, Map<String, String>> {

	private final Settings settings;

	public SettingsLookupClient(Settings settings) {
		this.settings = settings;
	}

	@Override
	public Map<String, String> apply(DefaultPublicationService.SettingsLookup settingsLookup) {
		return settings.getAllValuesByCategory(settingsLookup.mogulId(), settingsLookup.category());
	}

}
