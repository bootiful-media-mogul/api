package com.joshlong.mogul.api.settings;

/**
 * one setting a {@link SettingsCategory} offers: what it's called, whether the category
 * works without it, how the settings page should show it, and what it means when it
 * hasn't been set.
 *
 * @param name the setting's key within its category
 * @param required whether the category is unusable until it has a value
 * @param type how the value should be entered and shown
 * @param defaultValue what an unset value means, or null if it means nothing
 */
public record SettingDefinition(String name, boolean required, Type type, String defaultValue) {

	public enum Type {

		/** a credential: entered like a password, never shown in the clear */
		SECRET,

		/** plain text */
		TEXT,

		/** {@code true} or {@code false} */
		BOOLEAN

	}

	public static SettingDefinition secret(String name, boolean required) {
		return new SettingDefinition(name, required, Type.SECRET, null);
	}

	public static SettingDefinition bool(String name, boolean defaultValue) {
		return new SettingDefinition(name, false, Type.BOOLEAN, Boolean.toString(defaultValue));
	}

}
