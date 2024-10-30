package com.joshlong.mogul.api.utils;

import org.springframework.aot.hint.TypeReference;

import java.util.Set;

import static org.springframework.ai.aot.AiRuntimeHints.findClassesInPackage;

/**
 * Defers to the utility class in Spring AI.
 */
public abstract class HintsUtils {

	public static Set<TypeReference> findAnnotatedClassesInPackage(String packageName) {
		return findClassesInPackage(packageName, (metadataReader, metadataReaderFactory) -> true);
	}

}