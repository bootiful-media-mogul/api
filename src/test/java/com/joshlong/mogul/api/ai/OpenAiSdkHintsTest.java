package com.joshlong.mogul.api.ai;

import org.junit.jupiter.api.Test;
import org.springframework.aot.hint.ExecutableMode;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.TypeReference;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;

import static org.assertj.core.api.Assertions.assertThat;

class OpenAiSdkHintsTest {

	@Test
	void keepsOnlyTheModelPackagesSpringAiUses() {
		assertThat(OpenAiSdkHints.isKept("com.openai.models.chat.completions.ChatCompletion")).isTrue();
		assertThat(OpenAiSdkHints.isKept("com.openai.models.audio.transcriptions.Transcription$Builder")).isTrue();
		assertThat(OpenAiSdkHints.isKept("com.openai.models.ResponseFormatJsonSchema$JsonSchema")).isTrue();
		assertThat(OpenAiSdkHints.isKept("com.openai.core.JsonValue")).isTrue();
		assertThat(OpenAiSdkHints.isKept("com.openai.services.blocking.ChatService")).isTrue();
		assertThat(OpenAiSdkHints.isKept("com.fasterxml.jackson.databind.ser.Serializers")).isTrue();
		assertThat(OpenAiSdkHints.isKept("com.openai.models.beta.assistants.Assistant")).isFalse();
		assertThat(OpenAiSdkHints.isKept("com.openai.models.admin.organization.Project")).isFalse();
		assertThat(OpenAiSdkHints.isKept("com.openai.models.responses.Response")).isFalse();
		assertThat(OpenAiSdkHints.isKept("com.openai.models.realtime.RealtimeSession")).isFalse();
	}

	@Test
	void readsArrayTypesTheWayTheAgentWritesThem() {
		assertThat(OpenAiSdkHints.toCanonicalName("[B")).isEqualTo("byte[]");
		assertThat(OpenAiSdkHints.toCanonicalName("[[J")).isEqualTo("long[][]");
		assertThat(OpenAiSdkHints.toCanonicalName("[Ljava.lang.String;")).isEqualTo("java.lang.String[]");
		assertThat(OpenAiSdkHints.toCanonicalName("java.lang.String")).isEqualTo("java.lang.String");
	}

	@Test
	void registersTheKeptEntriesFromTheSdksOwnFile() {
		var hints = new RuntimeHints();
		new OpenAiSdkHints().registerHints(hints, getClass().getClassLoader());
		var reflection = RuntimeHintsPredicates.reflection();

		var chatCompletion = TypeReference.of("com.openai.models.chat.completions.ChatCompletion");
		assertThat(hints.reflection().getTypeHint(chatCompletion)).isNotNull();
		assertThat(reflection.onType(chatCompletion).withMemberCategory(MemberCategory.ACCESS_DECLARED_FIELDS))
			.accepts(hints);

		var assistant = TypeReference.of("com.openai.models.beta.assistants.Assistant");
		assertThat(hints.reflection().getTypeHint(assistant)).isNull();

		// an entry carried over with its individual methods
		assertThat(hints.reflection().typeHints().flatMap(type -> type.methods()).toList()).isNotEmpty()
			.allMatch(method -> method.getMode() == ExecutableMode.INVOKE);
	}

}
