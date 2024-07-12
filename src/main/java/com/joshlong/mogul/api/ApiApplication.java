package com.joshlong.mogul.api;

import com.joshlong.mogul.api.mogul.Mogul;
import com.joshlong.mogul.api.mogul.MogulCreatedEvent;
import org.apache.commons.logging.LogFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

import javax.sql.DataSource;
import java.time.format.DateTimeFormatter;
import java.util.Set;

import static org.springframework.security.config.Customizer.withDefaults;

@IntegrationComponentScan
@ImportRuntimeHints(ApiApplication.Hints.class)
@EnableConfigurationProperties(ApiProperties.class)
@SpringBootApplication
public class ApiApplication {

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			var mcs = MemberCategory.values();
			for (var c : Set.of(Mogul.class, MogulCreatedEvent.class))
				hints.reflection().registerType(c, mcs);
		}

	}

	@Bean
	ApplicationRunner validateDatabase(DataSource db) {
		return args -> {
			var count = JdbcClient.create(db).sql("select count(*) from mogul").query(Long.class).single();
			LogFactory.getLog(ApiApplication.this.getClass()).info("the count is " + count);
		};
	}

	public static void main(String[] args) {
		if (System.getenv("DEBUG") != null && System.getenv("DEBUG").equals("true"))
			System.getenv().forEach((k, v) -> System.out.println(k + "=" + v));
		SpringApplication.run(ApiApplication.class, args);
	}

	@Bean
	SecurityFilterChain jwtSecurityFilterChain(HttpSecurity http) throws Exception {
		http //
			.authorizeHttpRequests((authorize) -> authorize //
				.requestMatchers(EndpointRequest.toAnyEndpoint())
				.permitAll() //
				.anyRequest()
				.authenticated()//
			)//
			.oauth2ResourceServer((resourceServer) -> resourceServer.jwt(withDefaults()));
		return http.build();
	}

	@Bean
	DateTimeFormatter dateTimeFormatter() {
		return DateTimeFormatter.BASIC_ISO_DATE;
	}

}
