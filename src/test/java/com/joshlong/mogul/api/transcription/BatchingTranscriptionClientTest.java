package com.joshlong.mogul.api.transcription;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.FileSystemResource;

import java.io.File;

class BatchingTranscriptionClientTest {

	@Test
	void batch() throws Exception {
		var tbc = new BatchingTranscriptionClient();
		var home = new File(System.getenv("HOME"), "/Desktop/3-interview-export.mp3");
		var fileResource = new FileSystemResource(home);
		var transcriptionBatch = tbc.prepare(fileResource);
		Assertions.assertNotNull(transcriptionBatch);
		Assertions.assertNotNull(transcriptionBatch.segments());
		Assertions.assertFalse(transcriptionBatch.segments().isEmpty());
		var order = 0;
		for (var segment : transcriptionBatch.segments()) {
			Assertions.assertNotNull(segment.audio());
			Assertions.assertTrue(segment.audio().exists());
			Assertions.assertEquals(order, segment.order());
			order += 1;
		}

	}

}