package com.joshlong.mogul.api.podcasts.production;

import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.podcasts.Episode;
import com.joshlong.mogul.api.podcasts.PodcastService;
import com.joshlong.mogul.api.utils.FileUtils;
import com.joshlong.mogul.api.utils.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * given a {@link com.joshlong.mogul.api.podcasts.Podcast}, turn this into a complete
 * audio file
 */

@Component
public class PodcastProducer {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ManagedFileService managedFileService;

	private final PodcastService podcastService;

	PodcastProducer(ManagedFileService managedFileService, PodcastService podcastService) {
		this.managedFileService = managedFileService;
		this.podcastService = podcastService;
	}

	public ManagedFile produce(Episode episode) {

		var workspace = (File) null;
		try {
			workspace = new File(Files.createTempDirectory("managed-files-for-podcast-production").toFile(),
					Long.toString(episode.id()));
			Assert.state(workspace.exists() || workspace.mkdirs(),
					"the workspace directory [" + workspace.getAbsolutePath() + "] does not exist");
			var episodeId = episode.id();
			var segments = this.podcastService.getPodcastEpisodeSegmentsByEpisode(episodeId);
			var segmentFiles = new ArrayList<File>();
			for (var s : segments) {
				var localFile = new File((File) null, UUID.randomUUID() + "_" + s.producedAudio().filename());
				try (var in = this.managedFileService.read(s.producedAudio().id()).getInputStream();
						var out = new FileOutputStream(localFile)) {
					FileCopyUtils.copy(in, out);
				}
				segmentFiles.add(localFile);
			}
			var producedWav = this.produce(workspace, segmentFiles.toArray(new File[0]));
			// todo should this be converted to an .mp3 now?
			var producedAudio = episode.producedAudio();
			this.managedFileService.write(producedAudio.id(), producedAudio.filename(),
					MediaType.parseMediaType(producedAudio.contentType()), producedWav);
			this.log.debug("writing [{}]", episode.id());
			this.podcastService.writePodcastEpisodeProducedAudio(episode.id(), producedAudio.id());
			this.log.debug("wrote [{}]", episode.id());
			return this.managedFileService.getManagedFile(producedAudio.id());
		} //
		catch (Throwable throwable) {
			throw new RuntimeException(throwable);
		} //
		finally {
			if (workspace != null) {
				Assert.state(!workspace.exists() || FileUtils.delete(workspace),
						"we could not delete the temporary directory [" + workspace.getAbsolutePath() + "]");
			}
		}
	}

	private File ensureWav(File workspace, File input) {
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

	private File workspaceTempFile(File workspace, String ext) {
		return new File(workspace, UUID.randomUUID() + (ext.startsWith(".") ? ext : "." + ext));
	}

	private File produce(File workspace, File... audioFiles) throws Exception {
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

}
