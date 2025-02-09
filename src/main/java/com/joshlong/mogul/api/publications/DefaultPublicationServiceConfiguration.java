package com.joshlong.mogul.api.publications;

import com.joshlong.mogul.api.PublishableRepository;
import com.joshlong.mogul.api.Settings;
import com.joshlong.mogul.api.mogul.MogulService;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Collection;

@Configuration
class DefaultPublicationServiceConfiguration {

	@Bean
	DefaultPublicationService defaultPublicationService(JdbcClient client, MogulService mogulService,
			ApplicationEventPublisher applicationEventPublisher, TransactionTemplate transactionTemplate,
			TextEncryptor textEncryptor, Settings settings,
			Collection<PublishableRepository<?>> publishableRepositories) {
		var lookup = new SettingsLookupClient(settings);
		return new DefaultPublicationService(client, mogulService, textEncryptor, transactionTemplate, lookup,
				applicationEventPublisher, publishableRepositories);
	}

}
