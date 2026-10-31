package com.joshlong.mogul.api.cleanvoice;

import com.joshlong.cleanvoice.Cleanvoice;
import com.joshlong.cleanvoice.CleanvoiceClient;
import com.joshlong.cleanvoice.CleanvoiceProperties;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.settings.Settings;
import com.joshlong.mogul.storage.Storage;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

// jobrunr stores each poll request as json, and reads it back to run it
@RegisterReflectionForBinding(CleanvoiceEditPollRequest.class)
@Configuration
class CleanvoiceConfiguration {

	@Bean
	CleanvoiceSettings cleanvoiceSettings(Settings settings) {
		return new CleanvoiceSettings(settings);
	}

	/**
	 * being a {@link Cleanvoice}, this is what the cleanvoice-client auto-configuration
	 * backs off for, so there's no application-wide key to configure.
	 */
	@Bean
	MultitenantCleanvoice cleanvoice(RestClient.Builder restClientBuilder, CleanvoiceProperties properties,
			CleanvoiceSettings cleanvoiceSettings, MogulService mogulService) {
		return new MultitenantCleanvoice(apiKey -> CleanvoiceClient.create(restClientBuilder, apiKey,
				properties.baseUrl(), properties.polling()), cleanvoiceSettings, mogulService);
	}

	@Bean
	DefaultAudioCleaning audioCleaning(Cleanvoice cleanvoice, Storage storage, ObjectProvider<JobRequestScheduler> jobs,
			ApplicationEventPublisher publisher, TransactionTemplate transactions) {
		return new DefaultAudioCleaning(cleanvoice, storage, jobs, publisher, transactions);
	}

	@Bean
	CleanvoiceEditPollHandler cleanvoiceEditPollHandler(Cleanvoice cleanvoice, ObjectProvider<JobRequestScheduler> jobs,
			ApplicationEventPublisher publisher, TransactionTemplate transactions) {
		return new CleanvoiceEditPollHandler(cleanvoice, jobs, publisher, transactions);
	}

}
