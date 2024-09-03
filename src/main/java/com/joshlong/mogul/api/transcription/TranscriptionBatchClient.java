package com.joshlong.mogul.api.transcription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.*;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class TranscriptionBatchClient {

    // we want files that are this size or smaller
    private static final long MAX_FILE_SIZE = 20 * 1024 * 1024;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<String, Pattern> patterns = new ConcurrentHashMap<>();

    private final File root = new File(System.getenv("HOME"), "Desktop/transcription");

	TranscriptionBatchClient() {
		Assert.state(this.root.exists() || this.root.mkdirs(),
				"the root for transcription, " + this.root.getAbsolutePath() + ", could not be created");
	}

	TranscriptionBatch prepare(Resource audio) throws Exception {

		// 1. find duration/size of the file
		// 2. find gaps/silence in the audio file.
        // 3. find the gap in the file nearest to the appropriate divided timecode
		// 4. divide the file into 20mb chunks.

		var transcriptionForResource = new File(root, UUID.randomUUID().toString());
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
        var parts = (int) (sizeInMb <= MAX_FILE_SIZE ? 1 : sizeInMb / MAX_FILE_SIZE);
        if (sizeInMb % MAX_FILE_SIZE != 0) {
            parts += 1;
        }

        Assert.state(parts > 0, "there can not be zero parts. this won't work!");
        var durationOfASinglePart = duration.toMillis() / parts;
        var rangesOfSilence = new long[parts];
        for (var indx = 0; indx < rangesOfSilence.length; indx++)
            rangesOfSilence[indx] = (1 + indx) * durationOfASinglePart;
        var cuts = new ArrayList<Silence>(); // we should have no more than _parts_ elements in this collection
        for (var r : rangesOfSilence) {
            cuts.add(this.lastBefore(r == 0 ? 0 : r / 1000, silentGapsInAudio));
        }
        
        // 4. do the actual file slicing 
        
        return new TranscriptionBatch(List.of());
	}

    private Silence lastBefore(long start, Silence[] silences) {
        Assert.state(silences.length > 0, "there should be more than one silence gap");
        var last = silences[0];
        var indx = 0;
        for (var s : silences) {
            if (indx != 0)
                last = silences[indx - 1];
            if (s.start() >= start) return last;
            indx += 1;
        }
        throw new IllegalStateException("could not find a segment that starts ");
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
                .command("ffmpeg", "-i", audio.getAbsolutePath(), "-af", "silencedetect=noise=-30dB:d=0.5",
                        "-f", "null", silence.getAbsolutePath())
                .inheritIO()
                .redirectOutput(ProcessBuilder.Redirect.PIPE)
                .redirectError(ProcessBuilder.Redirect.PIPE)
                .start();
        Assert.state(result.waitFor() == 0, "the result of silence detection should be 0, or good.");
        try (var output = new InputStreamReader(result.getErrorStream())) {
            var content = FileCopyUtils.copyToString(output);
            var silenceDetectionLogLines = Stream
                    .of(content.split(System.lineSeparator()))
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
            // [silencedetect @ 0x600000410000] silence_end: 19.178 | silence_duration: 0.506292
            for (var silenceDetectionLogLine : silenceDetectionLogLines) {
                if (offset % 2 == 0) {
                    start = Float.parseFloat(numberFor("silence_start", silenceDetectionLogLine));
                } // 
                else {
                    var pikeCharIndex = silenceDetectionLogLine.indexOf("|");
                    Assert.state(pikeCharIndex != -1, "the '|' character was not found");
                    var before = silenceDetectionLogLine.substring(0, pikeCharIndex);
                    var after = silenceDetectionLogLine.substring(1 + pikeCharIndex);
                    stop = Float.parseFloat(numberFor("silence_end", before));
                    duration = Float.parseFloat(numberFor("silence_duration", after));
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
                """
                .trim()
                .replace("XXXX", prefix);
        var pattern = this.patterns.computeIfAbsent(regex, r -> Pattern.compile(regex));
        var matcher = pattern.matcher(line);
        if (matcher.find())
            return matcher.group();
        throw new IllegalArgumentException("line [" + line + "] does not match pattern [" + pattern.pattern() + "]");

    }

    record Silence(float start,
                   float end,
                   float duration) {
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
