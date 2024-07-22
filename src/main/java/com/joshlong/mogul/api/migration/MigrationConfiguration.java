package com.joshlong.mogul.api.migration;

import com.joshlong.mogul.api.managedfiles.Storage;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.client.GraphQlClient;
import org.springframework.graphql.client.HttpSyncGraphQlClient;
import org.springframework.http.HttpHeaders;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.client.RestClient;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicReference;

@Configuration
class MigrationConfiguration {

	private static final String API_ROOT = "http://127.0.0.1:8080";

	static final AtomicReference<String> TOKEN = new AtomicReference<>();

	private final JdbcClient oldDb, newDb;

	MigrationConfiguration(DataSource dataSource, @Value("${legacy.db.username:mogul}") String username,
			@Value("${legacy.db.password:mogul}") String password, @Value("${legacy.db.host:localhost}") String host,
			@Value("${legacy.db.schema:legacy}") String schema) {
		this.oldDb = JdbcClient.create(dataSource(username, password, host, schema));
		this.newDb = JdbcClient.create(dataSource);
	}

	@Bean
	Migration migration(Storage storage, ApplicationEventPublisher publisher, OldApiClient oldApiClient,
			NewApiClient newApiClient) {
		return new Migration(storage, this.oldDb, this.newDb, publisher, oldApiClient, newApiClient);
	}

	@Bean
	HttpSyncGraphQlClient client() {
		var wc = RestClient.builder()
			.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN.get())
			.baseUrl(API_ROOT + "/graphql")
			.build();
		return HttpSyncGraphQlClient.create(wc);
	}

	@Bean
	NewApiClient newApiClient(GraphQlClient graphQlClient, JdbcClient jdbcClient) {
		return new NewApiClient(graphQlClient, jdbcClient);
	}

	@Bean
	OldApiClient oldApiClient(JdbcClient s) {
		return new OldApiClient(s);
	}

	private static DataSource dataSource(String username, String password, String host, String dbSchema) {
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
			.create(Migration.class.getClassLoader())
			.type(HikariDataSource.class)
			.driverClassName(jdbc.getDriverClassName())
			.url(jdbc.getJdbcUrl())
			.username(jdbc.getUsername())
			.password(jdbc.getPassword())
			.build();
	}

}
