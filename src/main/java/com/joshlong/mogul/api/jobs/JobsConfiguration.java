package com.joshlong.mogul.api.jobs;

import org.jspecify.annotations.Nullable;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Map;
import java.util.Set;

@Configuration
@ImportRuntimeHints(JobsConfiguration.Hints.class)
class JobsConfiguration {

	@Bean
	DefaultJobs jobs(Map<String, Job> jobsMap, JobRequestScheduler jobScheduler) {
		return new DefaultJobs(jobsMap, jobScheduler);
	}

	@Bean
	MogulJobRequestHandler mogulJobRequestHandler(Map<String, Job> jobsMap, ApplicationEventPublisher publisher,
			TransactionTemplate transactionTemplate) {
		return new MogulJobRequestHandler(jobsMap, publisher, transactionTemplate);
	}

	/**
	 * jobrunr writes every job request to the database as json, reads it back, and calls
	 * the handler's {@code run} by reflection. the native image has none of that unless
	 * it's told: without it, launching any job fails with "record components not
	 * available for record class MogulJobRequest".
	 */
	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {
			for (var clazz : Set.of(MogulJobRequest.class, MogulJobRequestHandler.class))
				hints.reflection().registerType(clazz, MemberCategory.values());
		}

	}

}
