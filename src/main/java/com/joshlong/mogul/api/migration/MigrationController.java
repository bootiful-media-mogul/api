package com.joshlong.mogul.api.migration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Controller;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <a href=
 * "https://github.com/spring-tips/spring-graphql-redux/blob/main/live/5-clients/src/main/java/com/example/clients/ClientsApplication.java">
 * see this page for examples on using the Spring GraphQL client</a>
 */
@Controller
class MigrationController {

	private final Log log = LogFactory.getLog(getClass());

	private final Migration migration;

	private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	MigrationController(Migration migration) {
		this.migration = migration;
	}

	@MutationMapping
	void migrate() {
		var authentication = (JwtAuthenticationToken) SecurityContextHolder //
			.getContextHolderStrategy() //
			.getContext() //
			.getAuthentication();

		var mogul = SecurityContextHolder.getContextHolderStrategy().getContext();
		var token = authentication.getToken().getTokenValue();
		this.executor.submit(() -> {
			try {
				SecurityContextHolder.getContextHolderStrategy().setContext(mogul);
				this.migration.migrateAllPodcasts(false, token);
				log.info("finished migration!");
			} //
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

}
