package com.joshlong.mogul.api.cleanvoice;

import com.joshlong.mogul.api.settings.SettingDefinition;
import com.joshlong.mogul.api.settings.Settings;
import com.joshlong.mogul.api.settings.SettingsCategory;
import org.springframework.util.StringUtils;

import java.util.Set;

/**
 * the cleanvoice section of the settings page, and the one place that knows what its
 * values mean.
 */
public class CleanvoiceSettings implements SettingsCategory {

	public static final String CATEGORY = "cleanvoice";

	static final String API_KEY = "apiKey";

	static final String APPLY_DEFAULT_FILTERS = "applyDefaultFilters";

	private static final boolean APPLY_DEFAULT_FILTERS_DEFAULT = true;

	private final Settings settings;

	CleanvoiceSettings(Settings settings) {
		this.settings = settings;
	}

	@Override
	public String category() {
		return CATEGORY;
	}

	@Override
	public Set<SettingDefinition> settings() {
		// optional: without a key, audio just isn't cleaned
		return Set.of(SettingDefinition.secret(API_KEY, false),
				SettingDefinition.bool(APPLY_DEFAULT_FILTERS, APPLY_DEFAULT_FILTERS_DEFAULT));
	}

	/**
	 * the mogul's cleanvoice api key, or null if they haven't given us one.
	 */
	public String apiKey(Long mogulId) {
		var key = this.settings.getValue(mogulId, CATEGORY, API_KEY);
		return StringUtils.hasText(key) ? key.trim() : null;
	}

	public boolean configured(Long mogulId) {
		return this.apiKey(mogulId) != null;
	}

	/**
	 * whether a freshly cleaned segment's produced audio should have the default filters
	 * applied -- fillers and stutters shortened, long silences trimmed -- before anyone
	 * has opened the editor.
	 */
	public boolean applyDefaultFilters(Long mogulId) {
		var value = this.settings.getValue(mogulId, CATEGORY, APPLY_DEFAULT_FILTERS);
		return StringUtils.hasText(value) ? Boolean.parseBoolean(value.trim()) : APPLY_DEFAULT_FILTERS_DEFAULT;
	}

}
