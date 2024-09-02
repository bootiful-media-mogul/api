package com.joshlong.mogul.api.transcription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.*;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TranscriptionBatchClient {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final File root = new File(System.getenv("HOME"), "Desktop/transcription");

	TranscriptionBatchClient() {
		Assert.state(this.root.exists() || this.root.mkdirs(),
				"the root for transcription, " + this.root.getAbsolutePath() + ", could not be created");
	}

	private void copy(InputStream inp, OutputStream outp) {
		try (var in = inp; var out = outp) {
			FileCopyUtils.copy(in, out);
		} //
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	TranscriptionBatch prepare(Resource audio) throws Exception {

		// 1. find duration/size of the file
		// 2. find gaps/silence in the audio file.
		// 3. find the gap in the file nearest to the appropriate timecode.
		// 4. divide the file into 20mb chunks.

		var transcriptionForResource = new File(root, UUID.randomUUID().toString());
		var originalAudio = new File(transcriptionForResource, "audio.mp3");
		Assert.state(transcriptionForResource.mkdirs(),
				"the directory [" + transcriptionForResource.getAbsolutePath() + "] has not been created");
		this.copy(audio.getInputStream(), new FileOutputStream(originalAudio));

		// 1. find duration/size of the file
		var sizeInMb = audio.contentLength();
		var duration = this.durationFor(originalAudio);
		this.log.debug("duration in timecode: {}; size in mb: {}", duration, sizeInMb);

		// 2. find gaps/silence in the audio file.

		return new TranscriptionBatch(List.of());
	}

	private Duration durationFor(File originalAudio) throws Exception {
		var durationOfFileExecution = Runtime.getRuntime()
			.exec(new String[] { "ffmpeg", "-i", originalAudio.getAbsolutePath() });
		durationOfFileExecution.waitFor();
		try (var content = new InputStreamReader(durationOfFileExecution.getErrorStream())) {
			var output = FileCopyUtils.copyToString(content);
			var durationPrefix = "Duration:";
			var duration = Stream.of(output.split(System.lineSeparator()))
				.filter(line -> line.contains(durationPrefix))
				.map(line -> line.split(durationPrefix)[1].split(",")[0])
				.collect(Collectors.joining(""))
				.trim();
			return durationFromTimecode(duration);
		}

	}

	private Duration durationFromTimecode(String tc) {
		var timecode = tc.lastIndexOf(".") == -1 ? tc : tc.substring(0, tc.lastIndexOf("."));
		try {
			var parts = timecode.split(":");
			var hours = Integer.parseInt(parts[0]) * 60 * 60 * 1000;
			var mins = Integer.parseInt(parts[1]) * 60 * 1000;
			var secs = Integer.parseInt(parts[2]) * 1000;
			return Duration.ofMillis(hours + mins + secs);
		} //
		catch (DateTimeParseException e) {
			throw new IllegalStateException("can't parse the date ", e);
		}
	}

}
