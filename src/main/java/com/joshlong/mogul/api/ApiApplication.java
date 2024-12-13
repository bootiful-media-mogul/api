package com.joshlong.mogul.api;

import com.joshlong.mogul.api.mogul.Mogul;
import com.joshlong.mogul.api.mogul.MogulCreatedEvent;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

import java.time.format.DateTimeFormatter;
import java.util.Set;

import static org.springframework.security.config.Customizer.withDefaults;

@IntegrationComponentScan
@ImportRuntimeHints(ApiApplication.Hints.class)
@EnableConfigurationProperties(ApiProperties.class)
@SpringBootApplication
public class ApiApplication {

	public static void main(String[] args) {
		SpringApplication.run(ApiApplication.class, args);
	}

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			var mcs = MemberCategory.values();
			for (var c : Set.of(Mogul.class, MogulCreatedEvent.class))
				hints.reflection().registerType(c, mcs);
		}

	}

	@Bean
	SecurityFilterChain jwtSecurityFilterChain(HttpSecurity http) throws Exception {
		return http //
			.authorizeHttpRequests((authorize) -> authorize //
				.requestMatchers(EndpointRequest.toAnyEndpoint())
				.permitAll() //
				.requestMatchers("/public/**")
				.permitAll()
				.anyRequest()
				.authenticated()//
			)//
			.oauth2ResourceServer(resourceServer -> resourceServer.jwt(withDefaults()))//
			.build();
	}

	@Bean
	DateTimeFormatter dateTimeFormatter() {
		return DateTimeFormatter.BASIC_ISO_DATE;
	}

}
