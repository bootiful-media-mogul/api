package com.joshlong.mogul.api.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.Settings;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.managedfiles.Storage;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.podbean.PodbeanClient;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.client.HttpSyncGraphQlClient;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@EnableAsync
@Configuration
class MigrationConfiguration {

	private static final String API_ROOT = "http://127.0.0.1:8080";

	static final AtomicReference<String> TOKEN = new AtomicReference<>();

	private final JdbcClient oldDb, newDb;

	MigrationConfiguration(DataSource dataSource, //
			@Value("${legacy.db.username:mogul}") String username, //
			@Value("${legacy.db.password:mogul}") String password, //
			@Value("${legacy.db.host:localhost}") String host, //
			@Value("${legacy.db.schema:legacy}") String schema //
	) {
		this.oldDb = JdbcClient.create(dataSource(username, password, host, schema));
		this.newDb = JdbcClient.create(dataSource);
	}

	@Bean
	Migration migration(Storage storage, ApplicationEventPublisher publisher, OldApiClient oldApiClient,
			ManagedFileService mfs, NewApiClient newApiClient, MogulService mogulService, TextEncryptor textEncryptor,
			Map<String, PublisherPlugin<?>> pluginMap, Settings settings, ObjectMapper objectMapper,
			PodbeanClient podbeanClient) {

		return new Migration(mfs, storage, this.newDb, publisher, oldApiClient, newApiClient, mogulService, settings,
				textEncryptor, pluginMap, objectMapper, podbeanClient);

	}

	static final String MIGRATION_REST_CLIENT_QUALIFIER = "migrationRestClient";

	static final String MIGRATION_REST_TEMPLATE_QUALIFIER = "migrationRestTemplate";

	@Bean(MIGRATION_REST_TEMPLATE_QUALIFIER)
	RestTemplate migrationRestTemplate(RestTemplateBuilder builder) {
		return builder//
			.rootUri(API_ROOT)
			.requestFactory(JdkClientHttpRequestFactory.class)
			.requestCustomizers(
					request -> request.getHeaders().put(HttpHeaders.AUTHORIZATION, List.of("Bearer " + TOKEN.get())))
			.build();
	}

	@Bean(MIGRATION_REST_CLIENT_QUALIFIER)
	RestClient migrationRestClient(RestClient.Builder builder) {
		return builder.baseUrl(API_ROOT + "/graphql")
			.requestFactory(new JdkClientHttpRequestFactory())
			.requestInitializer(
					request -> request.getHeaders().put(HttpHeaders.AUTHORIZATION, List.of("Bearer " + TOKEN.get())))
			.build();
	}

	@Bean
	HttpSyncGraphQlClient httpSyncGraphQlClient(
			@Qualifier(MIGRATION_REST_CLIENT_QUALIFIER) RestClient migrationRestClient) {
		return HttpSyncGraphQlClient.create(migrationRestClient);
	}

	@Bean
	NewApiClient newApiClient(HttpSyncGraphQlClient httpSyncGraphQlClient, JdbcClient jdbcClient,
			@Qualifier(MIGRATION_REST_CLIENT_QUALIFIER) RestClient restClient,
			@Qualifier(MIGRATION_REST_TEMPLATE_QUALIFIER) RestTemplate restTemplate) {
		return new NewApiClient(httpSyncGraphQlClient, jdbcClient, restClient, restTemplate);
	}

	@Bean
	OldApiClient oldApiClient() {
		return new OldApiClient(this.oldDb);
	}

	private DataSource dataSource(String username, String password, String host, String dbSchema) {
		var jdbc = new JdbcConnectionDetails() {
			@Override
			public String getUsername() {
				return username;
			}

			@Override
			public String getPassword() {
				return password;
			}

			@Override
			public String getJdbcUrl() {
				return "jdbc:postgresql://" + host + "/" + dbSchema;
			}
		};
		return DataSourceBuilder //
			.create(getClass().getClassLoader())
			.type(HikariDataSource.class)
			.driverClassName(jdbc.getDriverClassName())
			.url(jdbc.getJdbcUrl())
			.username(jdbc.getUsername())
			.password(jdbc.getPassword())
			.build();
	}

}
