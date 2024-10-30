package com.joshlong.mogul.api.feeds;

import com.joshlong.mogul.api.utils.HintsUtils;
import com.rometools.rome.feed.synd.SyndEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * registers GraalVM AOT hints for ROME RSS/ATOM feeds.
 */
class FeedRuntimeHintsRegistrar implements RuntimeHintsRegistrar {

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public void registerHints(RuntimeHints hints, ClassLoader classLoader) {

		if (!ClassUtils.isPresent("com.rometools.rome.feed.WireFeed", FeedRuntimeHintsRegistrar.class.getClassLoader()))
			return;

		var mcs = MemberCategory.values();
		var classes = HintsUtils.findAnnotatedClassesInPackage("com.rometools.rome");
		for (var c : classes) {
			try {
				var cls = Class.forName(c.getName());
				if (Serializable.class.isAssignableFrom(cls)) {
					this.log.info("register {} for reflection/serialization.", c.getName());
					hints.serialization().registerType(c);
					hints.reflection().registerType(c, mcs);
				}
			} //
			catch (ClassNotFoundException e) {
				this.log.warn("could not find the class {} and got the following exception: {}", (Object) c,
						e.toString()); // don't care
			}
		}

		// rome
		for (var c : new Class<?>[] { Date.class, SyndEntry.class, com.rometools.rome.feed.module.DCModuleImpl.class })
			hints.reflection().registerType(c, mcs);

		var resource = new ClassPathResource("/com/rometools/rome/rome.properties");
		hints.resources().registerResource(resource);
		try (var in = resource.getInputStream()) {
			var props = new Properties();
			props.load(in);
			props.propertyNames().asIterator().forEachRemaining(pn -> {
				var clz = loadClasses((String) pn, props.getProperty((String) pn));
				clz.forEach(cn -> hints.reflection().registerType(TypeReference.of(cn), mcs));
			});
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static List<String> loadClasses(String propertyName, String propertyValue) {
		Assert.hasText(propertyName, "the propertyName must not be null");
		Assert.hasText(propertyValue, "the propertyValue must not be null");
		return Arrays //
			.stream((propertyValue.contains(" ")) ? propertyValue.split(" ") : new String[] { propertyValue }) //
			.map(String::trim)
			.filter(xValue -> !xValue.isBlank())
			.toList();

	}

}
