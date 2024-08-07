package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.Settings;
import com.joshlong.mogul.api.mogul.MogulService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.transaction.support.TransactionTemplate;

@Configuration
class DefaultPublicationServiceConfiguration {

	@Bean
	DefaultPublicationService defaultPublicationService(JdbcClient client, MogulService mogulService,
			TransactionTemplate tt, TextEncryptor textEncryptor, Settings settings) {
		var lookup = new SettingsLookupClient(settings);
		return new DefaultPublicationService(client, mogulService, textEncryptor, tt, lookup);
	}

}
