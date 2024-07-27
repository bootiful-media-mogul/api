package com.joshlong.mogul.api.migration;

import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.managedfiles.Storage;
import com.joshlong.mogul.api.utils.JsonUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Async;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.SystemPropertyUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Stream;

class Migration {

	private final Log log = LogFactory.getLog(getClass());

	private final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	private final String fsRoot = "${HOME}/Desktop/mogul-migration";

	private final File existingS3Files = new File(
			SystemPropertyUtils.resolvePlaceholders(this.fsRoot + "/existing-s3-files/"));

	private final File managedFileLog = new File(
			SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/enqueuedManagedFileWrites"));

	private final Set<ManagedFileWrite> managedFileWrites = new ConcurrentSkipListSet<>(
			Comparator.comparing(ManagedFileWrite::managedFileId));

	private final ManagedFileService managedFileService;

	private final JdbcClient destinationJdbcClient;

	private final Storage storage;

	private final ApplicationEventPublisher publisher;

	private final OldApiClient oldApiClient;

	private final NewApiClient newApiClient;

	Migration(ManagedFileService managedFileService, Storage storage, JdbcClient destinationJdbcClient,
			ApplicationEventPublisher publisher, OldApiClient oldApiClient, NewApiClient newApiClient) {
		this.managedFileService = managedFileService;
		this.storage = storage;
		this.publisher = publisher;
		this.oldApiClient = oldApiClient;
		this.newApiClient = newApiClient;
		this.destinationJdbcClient = destinationJdbcClient;

		Assert.state(this.existingS3Files.exists() || this.existingS3Files.mkdirs(),
				"the directory [" + this.existingS3Files.getAbsolutePath() + "] does not exist");
	}

	private ManagedFile writeManagedFileToS3(Long managedFileId, String originalFileName, Resource resource) {
		var mediaType = CommonMediaTypes.guess(resource);
		this.managedFileService.write(managedFileId, originalFileName, mediaType, resource);
		var updated = this.managedFileService.getManagedFile(managedFileId);
		Assert.state(updated.written(), "the managedFile is written!");
		return updated;
	}

	@Async
	@EventListener(ManagedFilesReadyForWriteEvent.class)
	void afterMigration() throws Exception {
		writePendingManagedFilesListener();
	}

	@EventListener({ ApplicationReadyEvent.class })
	void afterLoad() throws Exception {
		writePendingManagedFilesListener();
	}

	void writePendingManagedFilesListener() throws Exception {
		System.out.println("invoking..");
		if (!this.managedFileLog.exists()) {
			log.warn("the managed s3 log [" + this.managedFileLog.getAbsolutePath() + "] does not exist!");
			return;
		}
		var pendingManagedFileWrites = this.readPendingManagedFileWriteLog();
		var arrayOfCompletableFutures = new CompletableFuture[pendingManagedFileWrites.size()];
		var ctr = 0;
		for (var managedFileWrite : pendingManagedFileWrites) {
			var s3Source = managedFileWrite.s3();
			var managedFileToConnectItTo = managedFileWrite.managedFileId();
			arrayOfCompletableFutures[ctr] = CompletableFuture.runAsync(() -> {
				var file = this.download(s3Source);
				log.info("downloaded " + s3Source + " to " + file.getAbsolutePath() + '.');
				this.writeManagedFileToS3(managedFileToConnectItTo, file.getName(), new FileSystemResource(file));
			}, this.executor);
			ctr += 1;
		}
		CompletableFuture.allOf(arrayOfCompletableFutures);
	}

	private ArrayList<ManagedFileWrite> readPendingManagedFileWriteLog() throws IOException {
		Assert.state(this.managedFileLog.exists(),
				"the managed s3 log [" + this.managedFileLog.getAbsolutePath() + "] does not exist");
		var files = new ArrayList<ManagedFileWrite>();
		try (var fin = new BufferedReader(new FileReader(this.managedFileLog))) {
			var contents = FileCopyUtils.copyToString(fin);
			// @formatter:off
			var read = JsonUtils.read(contents,
				new ParameterizedTypeReference<Collection<Map<String, Object>>>() {});
			// @formatter:on
			for (var r : read) {
				var managedFileWrite = new ManagedFileWrite(((Integer) r.get("managedFileId")).longValue(),
						((String) r.get("s3")));
				files.add(managedFileWrite);
			}
		}
		log.debug("there are " + files.size() + " files to read and synchronize to S3");
		return files;
	}

	/**
	 * this gets called from online with access to a token
	 */
	void migrateAllPodcasts(boolean reset, String token) {
		try {
			MigrationConfiguration.TOKEN.set(token);
			var media = this.oldApiClient.media();
			var podcasts = this.oldApiClient.podcasts(media);
			log.info("podcasts.length = " + podcasts.size());
			if (reset) {
				Assert.state(!this.managedFileLog.exists() || this.managedFileLog.delete(),
						"the managed s3 log, " + this.managedFileLog.getAbsolutePath() + ", was not reset");
				var sql = """
							delete from managed_file_deletion_request ;
							delete from publication ;
							delete from podcast_episode_segment ;
							delete from podcast_episode ;
							delete from managed_file ;
							delete from event_publication ;
						""";
				this.destinationJdbcClient.sql(sql).update();
				log.info("removed the tables.");
			}

			var destinationPodcastId = this.newApiClient.getPodcastId();
			log.info("destinationPodcastId " + destinationPodcastId);

			for (var podcast : podcasts) {
				this.migrate(destinationPodcastId, podcast);
				log.info("migrated legacy podcast #" + podcast.id());
			}
			try (var fw = new BufferedWriter(new FileWriter(this.managedFileLog))) {
				fw.write(JsonUtils.write(this.managedFileWrites));
			}
			this.publisher.publishEvent(new ManagedFilesReadyForWriteEvent());

		} //
		catch (Throwable throwable) {
			log.error("no idea", throwable);
			throw new RuntimeException(throwable);
		}
	}

	private Predicate<OldApiClient.Media> mediaTagPredicate(String tag) {
		return media -> media != null && StringUtils.hasText(media.href()) && media.type().equalsIgnoreCase(tag);
	}

	private OldApiClient.Media matchMediaByTag(Collection<OldApiClient.Media> mediaCollection, String tag) {
		return mediaCollection.stream().filter(mediaTagPredicate(tag)).findAny().orElse(null);
	}

	private void migrate(Long newPodcastId, OldApiClient.Podcast oldEpisode) throws Exception {
		var podcastEpisodeDraftId = this.newApiClient.createPodcastEpisodeDraft(newPodcastId, oldEpisode.title(),
				oldEpisode.description(), oldEpisode.date());
		var media = oldEpisode.media();
		var photoMedia = this.matchMediaByTag(media, "photo");
		var introMedia = this.matchMediaByTag(media, "introduction");
		var interviewMedia = this.matchMediaByTag(media, "interview");
		var producedMedia = this.matchMediaByTag(media, "produced");
		var isProduced = producedMedia != null;
		var isSegmented = Stream.of(introMedia, interviewMedia)
			.allMatch(m -> m != null && StringUtils.hasText(m.href()));
		var hasValidFiles = isProduced || isSegmented;
		if (!hasValidFiles) {
			log.warn("we don't have valid files for podcast " + newPodcastId + ". Returning.");
			return;
		}

		log.info("have all the files locally...");
		var episode = this.newApiClient.podcastEpisodeById(podcastEpisodeDraftId);
		this.registerWrite(episode.graphic().id(), photoMedia.href());
		if (isProduced) {
			Assert.state(episode.segments() != null, "there should be at least one audio segment");
			var firstSegment = episode.segments()[0];
			var mf = firstSegment.audio();
			this.registerWrite(mf.id(), producedMedia.href());
			log.debug("wrote the ManagedFile for the produced audio.");
		} //
		else {
			var assetsRoot = new File(SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/misc/podcast-assets/"));
			Assert.state(assetsRoot.exists(), "the asset root exists.");
			var assetsS3Root = "s3://podcast-assets-bucket/062019/";
			var introAsset = assetsS3Root + "intro.mp3";
			var closingAsset = assetsS3Root + "closing.mp3";
			var segueAsset = assetsS3Root + "music-segue.mp3";

			var segmentFiles = new ArrayList<String>();
			segmentFiles.add(introAsset);
			segmentFiles.add((introMedia.href()));
			segmentFiles.add(segueAsset);
			segmentFiles.add((interviewMedia.href()));
			segmentFiles.add(closingAsset);

			var total = segmentFiles.size();

			var already = episode.segments().length;
			while (already < total) {
				this.newApiClient.addPodcastEpisodeSegment(episode.id());
				already += 1;
			}

			// refresh
			episode = this.newApiClient.podcastEpisodeById(episode.id());
			Assert.state(episode.segments().length == total, "there should be " + total + " segments and no more");
			var current = 0;

			for (var segmentFile : segmentFiles) {
				var segment = episode.segments()[current];
				this.registerWrite(segment.audio().id(), segmentFile);
				current += 1;
			}
		}
		log.debug("got the episode " + episode);
	}

	// record that we need to get an s3 artifact and upload it to the new managedFile
	// system..
	private void registerWrite(Long managedFileId, String s3) {
		var mfw = new ManagedFileWrite(managedFileId, s3);
		this.managedFileWrites.add(mfw);
	}

	private File download(String s3Url) {
		try {
			var parts = s3Url.split("/");
			var bucket = parts[2];
			var key = parts[3] + "/" + parts[4];
			var localFile = new File(this.existingS3Files, bucket + "/" + key);
			if (localFile.length() > 0)
				return localFile;

			var read = this.storage.read(bucket, key);

			if (null == read)
				return null;

			var cl = read.contentLength();

			if (!localFile.exists() || (cl != localFile.length())) {
				log.info("contentLength: " + cl + ":" + localFile.length());
				log.info("downloading [" + localFile.getAbsolutePath() + "] on [" + Thread.currentThread().getName()
						+ "].");
				var directory = localFile.getParentFile();
				if (!directory.exists())
					directory.mkdirs();
				Assert.state(directory.exists(), "the directory [" + directory.getAbsolutePath() + "] does not exist");

				read = this.storage.read(bucket, key); // don't want to work on the same
				try (var in = new BufferedInputStream(read.getInputStream());
						var out = new BufferedOutputStream(new FileOutputStream(localFile))) {
					FileCopyUtils.copy(in, out);
				}
				Assert.state(localFile.exists(), "the local s3 must exist now, but doesn't");
			}

			var good = localFile.exists() && localFile.isFile() && cl == localFile.length();
			if (!good)
				log.warn("could not download " + s3Url);

			return localFile;
		} //
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}

record ManagedFileWrite(Long managedFileId, String s3) {
}

record ManagedFilesReadyForWriteEvent() {
}