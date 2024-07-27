package com.joshlong.mogul.api.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Role;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;
import java.util.concurrent.atomic.AtomicReference;

/**
 * have Spring create an instance of this and we'll capture the {@link ObjectMapper om} in
 * a static variable.
 */
@Component
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class JsonUtils {

	private static final AtomicReference<ObjectMapper> OBJECT_MAPPER_ATOMIC_REFERENCE = new AtomicReference<>();

	JsonUtils(ObjectMapper objectMapper) {
		OBJECT_MAPPER_ATOMIC_REFERENCE.set(objectMapper);
	}

	public static <T> T read(String json, ParameterizedTypeReference<T> ptr) {
		var tr = new TypeReference<T>() {
			@Override
			public Type getType() {
				return ptr.getType();
			}
		};
		try {
			return OBJECT_MAPPER_ATOMIC_REFERENCE.get().readValue(json, tr);
		} //
		catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public static <T> T read(String json, Class<T> ptr) {
		try {
			return OBJECT_MAPPER_ATOMIC_REFERENCE.get().readValue(json, ptr);
		}
		catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public static String write(Object o) {
		try {
			return OBJECT_MAPPER_ATOMIC_REFERENCE.get().writeValueAsString(o);
		} //
		catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

}
