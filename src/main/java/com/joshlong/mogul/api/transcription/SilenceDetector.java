package com.joshlong.mogul.api.transcription;

import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

abstract class SilenceDetector {

	public record Silence(float start, float end, float duration) {
	}

	private static final Map<String, Pattern> PATTERNS = new ConcurrentHashMap<>();

	public static Silence[] detect(File audio) throws Exception {
		var silence = new File(audio.getParentFile(), "silence");
		var result = new ProcessBuilder()
			.command("ffmpeg", "-i", audio.getAbsolutePath(), "-af", "silencedetect=noise=-30dB:d=0.5", "-f", "null",
					silence.getAbsolutePath())
			.inheritIO()
			.redirectOutput(ProcessBuilder.Redirect.PIPE)
			.redirectError(ProcessBuilder.Redirect.PIPE)
			.start();
		Assert.state(result.waitFor() == 0, "the result of silence detection should be 0, or good.");
		try (var output = new InputStreamReader(result.getErrorStream())) {
			var content = FileCopyUtils.copyToString(output);
			var silenceDetectionLogLines = Stream.of(content.split(System.lineSeparator()))
				.filter(l -> l.contains("silencedetect"))
				.map(l -> l.split("]")[1])
				.toList();
			var silences = new ArrayList<Silence>();
			var offset = 0;

			// to be reused
			var start = 0f;
			var stop = 0f;
			var duration = 0f;

			// look for and parse lines that look like this:
			// [silencedetect @ 0x600000410000] silence_start: 18.671708
			// [silencedetect @ 0x600000410000] silence_end: 19.178 | silence_duration:
			for (var silenceDetectionLogLine : silenceDetectionLogLines) {
				if (offset % 2 == 0) {
					start = 1000 * Float.parseFloat(numberFor("silence_start", silenceDetectionLogLine));
				} //
				else {
					var pikeCharIndex = silenceDetectionLogLine.indexOf("|");
					Assert.state(pikeCharIndex != -1, "the '|' character was not found");
					var before = silenceDetectionLogLine.substring(0, pikeCharIndex);
					var after = silenceDetectionLogLine.substring(1 + pikeCharIndex);
					stop = 1000 * Float.parseFloat(numberFor("silence_end", before));
					duration = 1000 * Float.parseFloat(numberFor("silence_duration", after));
					silences.add(new Silence(start, stop, duration));
				}
				offset += 1;
			}
			return silences.toArray(new Silence[0]);
		}
	}

	private static String numberFor(String prefix, String line) {
		var regex = """
				    (?<=XXXX:\\s)\\d+(\\.\\d+)?
				""".trim().replace("XXXX", prefix);
		var pattern = PATTERNS.computeIfAbsent(regex, r -> Pattern.compile(regex));
		var matcher = pattern.matcher(line);
		if (matcher.find())
			return matcher.group();
		throw new IllegalArgumentException("line [" + line + "] does not match pattern [" + pattern.pattern() + "]");
	}

}
