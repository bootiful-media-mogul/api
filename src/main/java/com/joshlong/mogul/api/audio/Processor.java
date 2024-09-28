package com.joshlong.mogul.api.audio;

import com.joshlong.mogul.api.utils.ProcessUtils;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.SystemPropertyUtils;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * i had a crazy idea. is it finally time to rewrite the Python code and make it work in
 * Java? esp. if the work being done is just {@code ffmpeg} calls?
 * <p>
 * AI: grant me strength!
 */
public class Processor {

	// todo refactor this method delegate to AudioEncoder, which has exactly this same
	// code.
	private static File ensureWav(File workspace, File input) {
		try {
			var inputAbsolutePath = input.getAbsolutePath();
			Assert.state(input.exists() && input.isFile(),
					"the input ['" + inputAbsolutePath + "'] must be a valid, existing file");
			var ext = "wav";
			if (inputAbsolutePath.toLowerCase().endsWith(ext))
				return input;
			var wav = workspaceTempFile(workspace, ext);
			var wavAbsolutePath = wav.getAbsolutePath();
			var exit = ProcessUtils.runCommand("ffmpeg", "-i", inputAbsolutePath, "-acodec", "pcm_s16le", "-vn", "-f",
					"wav", wavAbsolutePath);
			Assert.state(exit == 0, "the ffmpeg command ran successfully");
			return wav;
		} //
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static File workspaceTempFile(File workspace, String ext) {
		return new File(workspace, UUID.randomUUID() + (ext.startsWith(".") ? ext : "." + ext));
	}

	private static File produce(File workspace, File... audioFiles) throws Exception {
		Assert.state((workspace.exists() && workspace.isDirectory()) || workspace.mkdirs(),
				"the folder root [" + workspace.getAbsolutePath() + "] does not exist");
		var fileNames = Arrays.stream(audioFiles)
			.parallel()
			.peek(file -> Assert.state(file.exists() && file.isFile(),
					"the file '" + file.getAbsolutePath() + "' does not exist"))
			.map(file -> (file.getAbsolutePath().toLowerCase(Locale.ROOT).endsWith("wav")) ? file
					: ensureWav(workspace, file))
			.map(File::getAbsolutePath)
			.map(path -> "file '" + path + "'")
			.collect(Collectors.joining(System.lineSeparator()));
		var filesFile = workspaceTempFile(workspace, "txt");
		try (var out = new FileWriter(filesFile)) {
			FileCopyUtils.copy(fileNames, out);
		}
		var producedWav = workspaceTempFile(workspace, "wav");
		ProcessUtils.runCommand("ffmpeg", "-f", "concat", "-safe", "0", "-i", filesFile.getAbsolutePath(), "-c", "copy",
				producedWav.getAbsolutePath());
		Assert.state(producedWav.exists(),
				"the produced audio at " + producedWav.getAbsolutePath() + " does not exist.");
		return producedWav;

	}

	public static void main(String[] args) throws Exception {
		var filePaths = ("${HOME}/Desktop/misc/sample-podcast/an-intro.mp3,"
				+ "${HOME}/Desktop/misc/sample-podcast/the-interview.mp3")
			.split(",");
		var files = new File[filePaths.length];
		var ctr = 0;
		for (var fn : filePaths) {
			var file = new File(SystemPropertyUtils.resolvePlaceholders(fn));
			Assert.state(file.exists(), "the file [" + file.getAbsolutePath() + "] does not exist");
			files[ctr++] = file;
		}
		var workspace = new File(SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/sample/"));
		var producedWav = produce(workspace, files);
		Assert.state(producedWav.exists(),
				"the produced .wav file [" + producedWav.getAbsolutePath() + "] does not exist");

	}

}
