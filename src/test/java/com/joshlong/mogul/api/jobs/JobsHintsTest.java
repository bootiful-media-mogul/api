package com.joshlong.mogul.api.jobs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.predicate.RuntimeHintsPredicates;
import org.springframework.context.annotation.ImportRuntimeHints;

import java.util.Arrays;
import java.util.List;

/**
 * the native image can only do what its hints allow: without these, launching any job in
 * production failed, since jobrunr couldn't turn a {@link MogulJobRequest} into json.
 */
class JobsHintsTest {

	private final RuntimeHints hints = new RuntimeHints();

	JobsHintsTest() {
		new JobsConfiguration.Hints().registerHints(this.hints, getClass().getClassLoader());
	}

	@Test
	void jobrunrCanWriteAndReadAJobRequest() {
		var reflection = RuntimeHintsPredicates.reflection();
		for (var component : MogulJobRequest.class.getRecordComponents())
			Assertions.assertTrue(
					reflection.onMethodInvocation(MogulJobRequest.class, component.getName()).test(this.hints),
					"jobrunr reads the record's " + component.getName());
		var constructor = MogulJobRequest.class.getDeclaredConstructors()[0];
		Assertions.assertTrue(reflection.onType(MogulJobRequest.class).test(this.hints));
		Assertions.assertTrue(reflection.onConstructorInvocation(constructor).test(this.hints),
				"jobrunr makes the record again, to run it");
	}

	@Test
	void jobrunrCanRunTheHandler() {
		Assertions.assertTrue(RuntimeHintsPredicates.reflection()
			.onMethodInvocation(MogulJobRequestHandler.class, "run")
			.test(this.hints));
	}

	@Test
	void theHintsAreImported() {
		var imported = JobsConfiguration.class.getAnnotation(ImportRuntimeHints.class);
		Assertions.assertNotNull(imported);
		Assertions.assertEquals(List.of(JobsConfiguration.Hints.class), Arrays.asList(imported.value()));
	}

}
