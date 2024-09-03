package com.joshlong.mogul.api.transcription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.*;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

interface TranscriptionClient {

	String transcribe(Resource audio);

}

class BatchingTranscriptionClient implements TranscriptionClient {

	// we want files that are this size or smaller
	private static final long MAX_FILE_SIZE = 10 * 1024 * 1024;

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Map<String, Pattern> patterns = new ConcurrentHashMap<>();

	private final ThreadLocal<NumberFormat> numberFormat = new ThreadLocal<>();

	private final File root;

	private final long maxFileSize;

	private final Function<Resource, String> transcriptionFunction;

	BatchingTranscriptionClient(long maxFileSize, File root, Function<Resource, String> transcriptionFunction) {
		this.root = root;
		this.maxFileSize = maxFileSize <= 0 ? MAX_FILE_SIZE : maxFileSize;
		this.transcriptionFunction = transcriptionFunction;
		Assert.notNull(this.transcriptionFunction, "the transcription function must never be null");
		Assert.state(this.root.exists() || this.root.mkdirs(),
				"the root for transcription, " + this.root.getAbsolutePath() + ", could not be created");
	}

	private List<TranscriptionSegment> prepare(Resource audio) throws Exception {

		Assert.state(audio != null && audio.exists() && audio.contentLength() > 0 && audio.isFile(),
				"the file must be a valid file");

		// special case if the file is small enough
		if (audio.contentLength() < this.maxFileSize)
			return List.of(new TranscriptionSegment(audio, 0, 0, durationFor(audio.getFile()).toMillis()));

		// 1. find duration/size of the file
		// 2. find gaps/silence in the audio file.
		// 3. find the gap in the file nearest to the appropriate divided timecode
		// 4. divide the file into 20mb chunks.

		var transcriptionForResource = new File(this.root, UUID.randomUUID().toString());
		var originalAudio = new File(transcriptionForResource, "audio.mp3");
		Assert.state(transcriptionForResource.mkdirs(),
				"the directory [" + transcriptionForResource.getAbsolutePath() + "] has not been created");
		this.copy(audio.getInputStream(), new FileOutputStream(originalAudio));

		// 1. find duration/size of the file
		var sizeInMb = audio.contentLength();
		var duration = this.durationFor(originalAudio);
		this.log.debug("duration in time code: {}; size in mb: {}", duration, sizeInMb);

		// 2. find gaps/silence in the audio file.
		var silentGapsInAudio = this.detectSilenceFor(originalAudio);
		this.log.info("the file length is {} and the gaps are {}", audio.contentLength(), silentGapsInAudio);

		// 3. find the gap in the file nearest to the appropriate divided timecode
		var parts = (int) (sizeInMb <= this.maxFileSize ? 1 : sizeInMb / this.maxFileSize);
		if (sizeInMb % this.maxFileSize != 0) {
			parts += 1;
		}

		Assert.state(parts > 0, "there can not be zero parts. this won't work!");
		var totalDurationInMillis = duration.toMillis();
		var durationOfASinglePart = totalDurationInMillis / parts; // 724333
		var rangesOfSilence = new long[1 + parts];

		this.log.info("duration of a single part:  {}", durationOfASinglePart);

		rangesOfSilence[0] = 0;

		for (var indx = 1; indx < rangesOfSilence.length; indx++)
			rangesOfSilence[indx] = (indx) * durationOfASinglePart;

		rangesOfSilence[rangesOfSilence.length - 1] = totalDurationInMillis;

		Assert.state(rangesOfSilence[rangesOfSilence.length - 1] + durationOfASinglePart >= totalDurationInMillis,
				"the last silence marker (plus individual duration of " + durationOfASinglePart
						+ " ) should be greater than (or at least equal to) the total duration of the entire audio clip, "
						+ durationOfASinglePart);

		this.log.info("all ranges: {}", rangesOfSilence);
		var ranges = new ArrayList<float[]>();
		for (var i = 1; i < rangesOfSilence.length; i += 1) {
			var range = new float[] { rangesOfSilence[i - 1], rangesOfSilence[i] };
			ranges.add(range);
		}

		var betterRanges = new ArrayList<float[]>();

		for (var range : ranges) {
			var start = findSilenceClosestTo(range[0], silentGapsInAudio).start();
			var stop = findSilenceClosestTo(range[1], silentGapsInAudio).start();
			var e = new float[] { start, stop };
			if (Arrays.equals(range, ranges.getFirst()))
				e[0] = 0;
			if (Arrays.equals(range, ranges.getLast()))
				e[1] = totalDurationInMillis;
			betterRanges.add(e);
		}

		// 4. divide the file into 20mb chunks.
		var indx = 0;
		var listOfSegments = new ArrayList<TranscriptionSegment>();
		var numberFormat = numberFormat(); // not thread safe. not cheap.
		for (var r : betterRanges) {
			var destinationFile = new File(transcriptionForResource, numberFormat.format(indx) + ".mp3");
			this.log.debug("the file name is  {}", destinationFile);
			var start = (long) r[0];
			var stop = (long) r[1];
			this.bisect(originalAudio, destinationFile, start, stop);
			listOfSegments.add(new TranscriptionSegment(new FileSystemResource(destinationFile), indx, start, stop));
			indx += 1;
		}

		return listOfSegments;
	}

	private static String convertMillisToTimeFormat(long millis) {
		var hours = TimeUnit.MILLISECONDS.toHours(millis);
		var minutes = TimeUnit.MILLISECONDS.toMinutes(millis) % 60;
		var seconds = TimeUnit.MILLISECONDS.toSeconds(millis) % 60;
		var milliseconds = millis % 1000;
		return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, milliseconds);
	}

	private void bisect(File source, File destination, long start, long stop) throws IOException, InterruptedException {
		var result = new ProcessBuilder()
			.command("ffmpeg", "-i", source.getAbsolutePath(), "-ss", convertMillisToTimeFormat(start), "-to",
					convertMillisToTimeFormat(stop), "-c", "copy", destination.getAbsolutePath())
			.inheritIO()
			.redirectOutput(ProcessBuilder.Redirect.PIPE)
			.redirectError(ProcessBuilder.Redirect.PIPE)
			.start();
		var exitCode = result.waitFor();
		Assert.state(exitCode == 0, "the result must be a zero exit code, but was [" + exitCode + "]");
	}

	private NumberFormat numberFormat() {
		// NumberFormats are not cheap and not thread safe, so keeping them in a
		// threadlocal
		if (this.numberFormat.get() == null) {
			var formatter = NumberFormat.getInstance();
			// i sincerely doubt we'll ever handle a file
			// with more than a billion segments... famous last words?
			formatter.setMinimumIntegerDigits(10);
			formatter.setGroupingUsed(false);
			this.numberFormat.set(formatter);
		}
		return this.numberFormat.get();
	}

	@Override
	public String transcribe(Resource audio) {
		var executor = Executors.newVirtualThreadPerTaskExecutor();

		class TranscriptionCallable implements Callable<String> {

			private final Resource audio;

			TranscriptionCallable(Resource audio) {
				this.audio = audio;
			}

			@Override
			public String call() throws Exception {
				return transcriptionFunction.apply(this.audio);
			}

		}

		try {
			var orderedAudio = this.prepare(audio).stream().map(tr -> new TranscriptionCallable(tr.audio())).toList();
			var futures = executor.invokeAll(orderedAudio);
			var strings = new ArrayList<String>();
			for (var f : futures)
				strings.add(f.get());
			return strings.stream().collect(Collectors.joining(System.lineSeparator()));
		} //
		catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	private record Silence(float start, float end, float duration) {
	}

	private Silence findSilenceClosestTo(float startToFind, Silence[] silences) {
		Assert.state(silences != null && silences.length > 0, "Silences array cannot be null or empty");
		var closestSilence = silences[0];
		var closestDistance = Math.abs(closestSilence.start() - startToFind);
		for (var silence : silences) {
			var currentDistance = Math.abs(silence.start() - startToFind);
			if (currentDistance < closestDistance) {
				closestSilence = silence;
				closestDistance = currentDistance;
			}
		}
		return closestSilence;
	}

	private void copy(InputStream inp, OutputStream outp) {
		try (var in = inp; var out = outp) {
			FileCopyUtils.copy(in, out);
		} //
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private Silence[] detectSilenceFor(File audio) throws Exception {
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

			// [silencedetect @ 0x600000410000] silence_start: 18.671708
			// [silencedetect @ 0x600000410000] silence_end: 19.178 | silence_duration:
			// 0.506292
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

	private String numberFor(String prefix, String line) {
		var regex = """
				    (?<=XXXX:\\s)\\d+(\\.\\d+)?
				""".trim().replace("XXXX", prefix);
		var pattern = this.patterns.computeIfAbsent(regex, r -> Pattern.compile(regex));
		var matcher = pattern.matcher(line);
		if (matcher.find())
			return matcher.group();
		throw new IllegalArgumentException("line [" + line + "] does not match pattern [" + pattern.pattern() + "]");

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
