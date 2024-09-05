package com.joshlong.mogul.api.transcription;

import com.joshlong.mogul.api.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.openai.OpenAiAudioTranscriptionModel;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * this is an implementation of {@link Transcriber transcription service} that divides
 * larger files into smaller ones and then transcribes each of those, aggregating all the
 * results into one big transcript. it also takes care to try to divine pauses - gaps of
 * silence - in the audio and cut along those gaps.
 *
 * @author Josh Long
 */
class ChunkingTranscriber implements Transcriber {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private static final ThreadLocal<NumberFormat> NUMBER_FORMAT = new ThreadLocal<>();

	private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	private final Map<Instant, Set<File>> filesToDelete = new ConcurrentHashMap<>();

	private final OpenAiAudioTranscriptionModel openAiAudioTranscriptionModel;

	private final File root;

	private final long maxFileSize;

	private final Runnable cleanup = () -> {
		var key = futureInstant(0);
		log.debug("the current hour instant is {}", key.toString());
		for (var file : filesToDelete.getOrDefault(key, new HashSet<>())) {
			FileUtils.delete(file);
		}
	};

	ChunkingTranscriber(OpenAiAudioTranscriptionModel openAiAudioTranscriptionModel, File root,
			long maxFileSizeInBytes) {
		this.openAiAudioTranscriptionModel = openAiAudioTranscriptionModel;
		this.root = root;
		this.maxFileSize = maxFileSizeInBytes;
		Assert.notNull(this.openAiAudioTranscriptionModel, "the openAiAudioTranscriptionModel must not be null");
		Assert.notNull(this.root, "the root directory must not be null");
		Assert.state(this.maxFileSize > 0, "the max file size must be greater than zero");
		Assert.state(this.root.exists() || this.root.mkdirs(),
				"the root for transcription, " + this.root.getAbsolutePath() + ", could not be created");
		Executors.newScheduledThreadPool(1).scheduleAtFixedRate(this.cleanup, 1, 10, TimeUnit.MINUTES);
	}

	@Override
	public String transcribe(Resource audio) {
		var transcriptionForResource = new File(this.root, UUID.randomUUID().toString());
		var parentFile = transcriptionForResource.getParentFile();
		Assert.state(parentFile.exists() || parentFile.mkdirs(), "the directory into which we're writing this file ["
				+ parentFile.getAbsolutePath() + "] does not exist, and could not be created.");
		try {
			var orderedAudio = this.divide(transcriptionForResource, audio)//
				.map(tr -> (Callable<String>) () -> this.openAiAudioTranscriptionModel.call(tr.audio()))//
				.toList();
			return this.executor//
				.invokeAll(orderedAudio)//
				.stream()//
				.map(ChunkingTranscriber::from)//
				.collect(Collectors.joining());
		} //
		catch (Exception e) {
			throw new RuntimeException(e);
		} //
		finally {
			FileUtils.delete(transcriptionForResource);
		}
	}

	private Instant futureInstant(int hour) {
		var now = ZonedDateTime.now(ZoneId.systemDefault());
		var hoursFromNow = now.plusHours(hour);
		var topOfTheHour = hoursFromNow.withMinute(0).withSecond(0).withNano(0);
		return topOfTheHour.toInstant();
	}

	private void enqueueForDeletion(File file) {
		var futureInstant = futureInstant(2);
		this.filesToDelete.computeIfAbsent(futureInstant, k -> new HashSet<>()).add(file);
	}

	private Stream<TranscriptionSegment> divide(File transcriptionForResource, Resource audio) throws Exception {

		// make sure we have the file locally
		var originalAudio = new File(transcriptionForResource, "audio.mp3");
		Assert.state(transcriptionForResource.mkdirs(),
				"the directory [" + transcriptionForResource.getAbsolutePath() + "] has not been created");
		FileCopyUtils.copy(audio.getInputStream(), new FileOutputStream(originalAudio));
		this.enqueueForDeletion(transcriptionForResource);
		var duration = this.durationFor(originalAudio);
		var sizeInBytes = originalAudio.length();

		// special case if the file is small enough
		if (originalAudio.length() < this.maxFileSize) {
			return Stream
				.of(new TranscriptionSegment(new FileSystemResource(originalAudio), 0, 0, duration.toMillis()));
		}

		// 1. find duration/size of the file
		// 2. find gaps/silence in the audio file.
		// 3. find the gap in the file nearest to the appropriate divided timecode
		// 4. divide the file into 20mb chunks.

		// 2. find gaps/silence in the audio file.
		var silentGapsInAudio = SilenceDetector.detect(originalAudio);

		// 3. find the gap in the file nearest to the appropriate divided timecode
		var parts = (int) (sizeInBytes <= this.maxFileSize ? 1 : sizeInBytes / this.maxFileSize);
		if (sizeInBytes % this.maxFileSize != 0) {
			parts += 1;
		}

		Assert.state(parts > 0, "there can not be zero parts. this won't work!");
		var totalDurationInMillis = duration.toMillis();
		var durationOfASinglePart = totalDurationInMillis / parts; // 724333
		var rangesOfSilence = new long[1 + parts];

		rangesOfSilence[0] = 0;

		for (var indx = 1; indx < rangesOfSilence.length; indx++)
			rangesOfSilence[indx] = (indx) * durationOfASinglePart;

		rangesOfSilence[rangesOfSilence.length - 1] = totalDurationInMillis;

		Assert.state(rangesOfSilence[rangesOfSilence.length - 1] + durationOfASinglePart >= totalDurationInMillis,
				"the last silence marker (plus individual duration of " + durationOfASinglePart
						+ " ) should be greater than (or at least equal to) the total duration of the entire audio clip, "
						+ durationOfASinglePart);

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

		// 4. divide the file into N-mb chunks.
		var indx = 0;
		var listOfSegments = new ArrayList<TranscriptionSegment>();
		var numberFormat = numberFormat(); // not thread safe. not cheap.
		for (var r : betterRanges) {
			var destinationFile = new File(transcriptionForResource, numberFormat.format(indx) + ".mp3");
			var start = (long) r[0];
			var stop = (long) r[1];
			this.bisect(originalAudio, destinationFile, start, stop);
			listOfSegments.add(new TranscriptionSegment(new FileSystemResource(destinationFile), indx, start, stop));
			indx += 1;
		}

		return listOfSegments.stream();
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
		if (NUMBER_FORMAT.get() == null) {
			var formatter = NumberFormat.getInstance();
			formatter.setMinimumIntegerDigits(10);
			formatter.setGroupingUsed(false);
			NUMBER_FORMAT.set(formatter);
		}
		return NUMBER_FORMAT.get();
	}

	private static <T> T from(Future<T> tFuture) {
		try {
			return tFuture.get();
		} //
		catch (Exception e) {
			throw new RuntimeException(e);
		}
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

	private SilenceDetector.Silence findSilenceClosestTo(float startToFind, SilenceDetector.Silence[] detectedSilence) {
		Assert.state(detectedSilence != null && detectedSilence.length > 0,
				"detectedSilence array cannot be null or empty");
		var closestSilence = detectedSilence[0];
		var closestDistance = Math.abs(closestSilence.start() - startToFind);
		for (var silence : detectedSilence) {
			var currentDistance = Math.abs(silence.start() - startToFind);
			if (currentDistance < closestDistance) {
				closestSilence = silence;
				closestDistance = currentDistance;
			}
		}
		return closestSilence;
	}

}
