package com.joshlong.mogul.api;

import graphql.scalars.ExtendedScalars;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.metrics.buffering.BufferingApplicationStartup;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.security.autoconfigure.actuate.web.servlet.EndpointRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.graphql.execution.RuntimeWiringConfigurer;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.resilience.annotation.EnableResilientMethods;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

@IntegrationComponentScan
@EnableResilientMethods
@EnableConfigurationProperties(ApiProperties.class)
@SpringBootApplication
public class ApiApplication {

	static void main(String[] args) {
		// pin the timezone so that it doesn't matter in which geozone we run the app
		TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
		var app = new SpringApplication(ApiApplication.class);
		app.setApplicationStartup(new BufferingApplicationStartup(1024 * 4));
		app.run(args);
	}

	@Bean
	Customizer<HttpSecurity> httpSecurityCustomizer() {
		return http -> http //
			.authorizeHttpRequests((authorize) -> authorize //
				.requestMatchers(EndpointRequest.toAnyEndpoint())
				.permitAll() //
				.requestMatchers("/public/**")
				.permitAll()//
			);
	}

	@Bean
	RuntimeWiringConfigurer runtimeWiringConfigurer() {
		return wiringBuilder -> wiringBuilder //
			.scalar(ExtendedScalars.Json)//
			.scalar(ExtendedScalars.DateTime)//
			.scalar(ExtendedScalars.Url)//
			.scalar(ExtendedScalars.Date);
	}

	@Bean
	DateTimeFormatter dateTimeFormatter() {
		return DateTimeFormatter.BASIC_ISO_DATE;
	}

}