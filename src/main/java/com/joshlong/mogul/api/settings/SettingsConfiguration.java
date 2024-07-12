package com.joshlong.mogul.api.settings;

import com.joshlong.mogul.api.ApiProperties;
import com.joshlong.mogul.api.Settings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.crypto.encrypt.Encryptors;
import org.springframework.security.crypto.encrypt.TextEncryptor;

@Configuration
class SettingsConfiguration {

	@Bean
	TextEncryptor textEncryptor(ApiProperties properties) {
		return Encryptors.text(properties.settings().password(), properties.settings().salt());
	}

	@Bean
	Settings settings(JdbcClient jdbcClient, TextEncryptor textEncryptor) {
		return new Settings(jdbcClient, textEncryptor);
	}

}
