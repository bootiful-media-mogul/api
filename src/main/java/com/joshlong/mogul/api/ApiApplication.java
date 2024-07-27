package com.joshlong.mogul.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.mogul.api.mogul.Mogul;
import com.joshlong.mogul.api.mogul.MogulCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.annotation.RegisterReflectionForBinding;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.http.ProblemDetail;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;

import java.security.Principal;
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

	public static void main(String[] args) {
		var env = System.getenv();
		if (env.get("DEBUG") != null && env.get("DEBUG").equals("true")) {
			env.forEach((k, v) -> System.out.println(k + "=" + v));
		}
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

@ControllerAdvice
@RegisterReflectionForBinding(ProblemDetail.class)
class AsyncRequestTimeoutExceptionHandler {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ObjectMapper objectMapper;

	AsyncRequestTimeoutExceptionHandler(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@ExceptionHandler
	void handle(AsyncRequestTimeoutException asyncRequestTimeoutException) throws Exception {
		log.info("=================================================");
		log.info("{}:{}", asyncRequestTimeoutException.getBody(), asyncRequestTimeoutException.getStatusCode());
		log.info(this.objectMapper.writeValueAsString(asyncRequestTimeoutException.getBody()));
	}

}
