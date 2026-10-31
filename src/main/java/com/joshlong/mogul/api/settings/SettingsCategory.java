package com.joshlong.mogul.api.settings;

import java.util.Set;

/**
 * a section of the settings page for something that isn't a publisher plugin, such as an
 * integration a feature uses on each mogul's behalf. a bean implementing this shows up on
 * the settings page, and its values are stored and encrypted like every other setting,
 * under {@link #category()}.
 */
public interface SettingsCategory {

	String category();

	Set<SettingDefinition> settings();

}
