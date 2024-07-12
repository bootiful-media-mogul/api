package com.joshlong.mogul.api.managedfiles;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.PollerFactory;
import org.springframework.messaging.support.MessageBuilder;

import java.time.Duration;
import java.util.Collection;

@Configuration
class ManagedFilesConfiguration {

	@Bean
	IntegrationFlow managedFileDeletionRequestsIntegrationFlow(ManagedFileService managedFileService) {

		var messageSource = (MessageSource<Collection<ManagedFileDeletionRequest>>) () -> MessageBuilder
			.withPayload(managedFileService.getOutstandingManagedFileDeletionRequests())
			.build();

		return IntegrationFlow
			.from(messageSource,
					pc -> pc.poller(pm -> PollerFactory.fixedRate(Duration.ofMinutes(1), Duration.ofMinutes(0))))
			.split()
			// this does the dirty work of deleting the bits from s3.
			.handle(ManagedFileDeletionRequest.class, (payload, headers) -> {
				managedFileService.completeManagedFileDeletion(payload.id());
				return null;
			})
			.get();
	}

}
