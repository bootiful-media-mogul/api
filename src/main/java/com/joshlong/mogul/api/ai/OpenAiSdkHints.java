package com.joshlong.mogul.api.ai;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aot.hint.ExecutableMode;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeHint;
import org.springframework.aot.hint.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Replaces the reflection metadata that the OpenAI Java SDK ({@code openai-java-core})
 * ships with a filtered copy of it.
 * <p>
 * The SDK's {@code META-INF/native-image/reflect-config.json} is agent-generated and
 * registers every model type in the whole OpenAI API (assistants, admin, realtime, evals,
 * fine-tuning, vector stores, and so on) for full reflection: about 12,600 entries, a
 * third of everything the image registers. Compiling all of that took the native build
 * past the memory a GitHub-hosted runner has. Spring AI only talks to the chat,
 * completions, audio, embeddings, images and moderations APIs, and the model types for
 * those never reference the other families, which are reachable only through
 * {@code OpenAIClient} accessors that Spring AI never calls.
 * <p>
 * The {@code native} build excludes the SDK's own file (see {@code --exclude-config} in
 * the {@code pom.xml}) and this registrar registers the same entries minus the
 * {@code com.openai.models} subpackages that aren't in {@link #KEPT_MODEL_PACKAGES}.
 * Reading the SDK's file at build time, rather than checking in a filtered copy, keeps
 * the metadata in step with whatever SDK version Spring AI brings in; an API family the
 * SDK adds later stays out until it's added to the list.
 */
class OpenAiSdkHints implements RuntimeHintsRegistrar {

	static final String SDK_JAR = "openai-java-core";

	static final String REFLECT_CONFIG = "META-INF/native-image/reflect-config.json";

	private static final String MODELS_PACKAGE = "com.openai.models.";

	/**
	 * the {@code com.openai.models} subpackages Spring AI uses. top-level types in
	 * {@code com.openai.models} itself are always kept.
	 */
	static final Set<String> KEPT_MODEL_PACKAGES = Set.of("chat", "completions", "audio", "embeddings", "images",
			"moderations");

	private static final Map<String, MemberCategory> CATEGORIES = Map.of( //
			"allDeclaredFields", MemberCategory.ACCESS_DECLARED_FIELDS, //
			"allPublicFields", MemberCategory.ACCESS_PUBLIC_FIELDS, //
			"queryAllDeclaredMethods", MemberCategory.INTROSPECT_DECLARED_METHODS, //
			"queryAllPublicMethods", MemberCategory.INTROSPECT_PUBLIC_METHODS, //
			"queryAllDeclaredConstructors", MemberCategory.INTROSPECT_DECLARED_CONSTRUCTORS, //
			"allDeclaredClasses", MemberCategory.DECLARED_CLASSES);

	private static final Set<String> KNOWN_KEYS = Set.of("name", "fields", "methods", "allDeclaredFields",
			"allPublicFields", "queryAllDeclaredMethods", "queryAllPublicMethods", "queryAllDeclaredConstructors",
			"allDeclaredClasses");

	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
		var entries = this.readSdkReflectConfig(classLoader);
		var kept = 0;
		for (var entry : entries) {
			var name = entry.get("name").asString();
			if (!isKept(name))
				continue;
			hints.reflection().registerType(TypeReference.of(toCanonicalName(name)), type -> this.apply(entry, type));
			kept += 1;
		}
		this.log.info("registered {} of the {} reflection entries {} ships", kept, entries.size(), SDK_JAR);
	}

	static boolean isKept(String typeName) {
		if (!typeName.startsWith(MODELS_PACKAGE))
			return true;
		var rest = typeName.substring(MODELS_PACKAGE.length());
		var dot = rest.indexOf('.');
		if (dot == -1) // a type in com.openai.models itself
			return true;
		return KEPT_MODEL_PACKAGES.contains(rest.substring(0, dot));
	}

	/**
	 * the agent writes array types the way {@link Class#getName()} does ({@code [B},
	 * {@code [Lfoo.Bar;}), and {@link TypeReference#of(String)} wants {@code byte[]} and
	 * {@code foo.Bar[]}.
	 */
	static String toCanonicalName(String name) {
		if (!name.startsWith("["))
			return name;
		var dimensions = 0;
		while (name.charAt(dimensions) == '[')
			dimensions += 1;
		var element = name.substring(dimensions);
		var component = switch (element) {
			case "Z" -> "boolean";
			case "B" -> "byte";
			case "C" -> "char";
			case "S" -> "short";
			case "I" -> "int";
			case "J" -> "long";
			case "F" -> "float";
			case "D" -> "double";
			default -> {
				if (element.startsWith("L") && element.endsWith(";"))
					yield element.substring(1, element.length() - 1);
				throw new IllegalArgumentException("can't read the array type " + name);
			}
		};
		return component + "[]".repeat(dimensions);
	}

	private void apply(JsonNode entry, TypeHint.Builder type) {
		for (var key : entry.propertyNames()) {
			if (!KNOWN_KEYS.contains(key))
				throw new IllegalStateException(
						"the " + SDK_JAR + " reflection entry for " + entry.get("name").asString() + " has '" + key
								+ "', which " + getClass().getSimpleName() + " doesn't know how to carry over");
			var category = CATEGORIES.get(key);
			if (category != null && entry.get(key).asBoolean())
				type.withMembers(category);
		}
		for (var field : entry.path("fields"))
			type.withField(field.get("name").asString());
		for (var method : entry.path("methods")) {
			var parameterTypes = new ArrayList<TypeReference>();
			for (var parameterType : method.path("parameterTypes"))
				parameterTypes.add(TypeReference.of(toCanonicalName(parameterType.asString())));
			var methodName = method.get("name").asString();
			if (methodName.equals("<init>"))
				type.withConstructor(parameterTypes, ExecutableMode.INVOKE);
			else
				type.withMethod(methodName, parameterTypes, ExecutableMode.INVOKE);
		}
	}

	/**
	 * the SDK's file has the same name as other jars' unnamespaced ones, so it's picked
	 * out by the jar it's in.
	 */
	private List<JsonNode> readSdkReflectConfig(ClassLoader classLoader) {
		try {
			var candidates = Collections.list(classLoader.getResources(REFLECT_CONFIG))
				.stream()
				.filter(url -> url.toString().contains("/" + SDK_JAR + "-"))
				.toList();
			if (candidates.size() != 1)
				throw new IllegalStateException("expected exactly one " + REFLECT_CONFIG + " in " + SDK_JAR
						+ " on the classpath, but found " + candidates);
			return this.read(candidates.getFirst());
		} //
		catch (IOException e) {
			throw new IllegalStateException("couldn't read " + SDK_JAR + "'s " + REFLECT_CONFIG, e);
		}
	}

	private List<JsonNode> read(URL url) throws IOException {
		try (InputStream in = url.openStream()) {
			var root = JsonMapper.builder().build().readTree(in);
			var entries = new ArrayList<JsonNode>();
			root.forEach(entries::add);
			return entries;
		}
	}

}
