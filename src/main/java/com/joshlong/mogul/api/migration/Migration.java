package com.joshlong.mogul.api.migration;

import com.joshlong.mogul.api.managedfiles.Storage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.modulith.events.ApplicationModuleListener;
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
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class Migration {

	private final Log log = LogFactory.getLog(getClass());

	private final File oldManagedFileSystem = new File(SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/old/"));

	private final Set<IncompleteFileEvent> incompleteFileEvents = new ConcurrentSkipListSet<>(
			Comparator.comparing(IncompleteFileEvent::s3Url));

	private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	private final JdbcClient sourceJdbcClient, destinationJdbcClient;

	private final Function<OldApiClient.Media, File> mediaDownloader;

	private final Storage storage;

	private final ApplicationEventPublisher publisher;

	private final OldApiClient oldApiClient;

	private final NewApiClient newApiClient;

	Migration(Storage storage, JdbcClient sourceJdbcClient, JdbcClient destinationJdbcClient,
			ApplicationEventPublisher publisher, OldApiClient oldApiClient, NewApiClient newApiClient) {
		this.storage = storage;
		this.publisher = publisher;
		this.oldApiClient = oldApiClient;
		this.newApiClient = newApiClient;
		this.sourceJdbcClient = sourceJdbcClient;
		this.destinationJdbcClient = destinationJdbcClient;
		this.mediaDownloader = media -> download(media.href());

		Assert.state(this.oldManagedFileSystem.exists() || this.oldManagedFileSystem.mkdirs(),
				"the directory [" + this.oldManagedFileSystem.getAbsolutePath() + "] does not exist");
	}

	String start(String token) throws Exception {

		MigrationConfiguration.TOKEN.set(token);
		var media = this.oldApiClient.media();
		var podcasts = this.oldApiClient.podcasts(media);
		var sql = """
				delete from managed_file_deletion_request ;
				delete from publication ;
				delete from podcast_episode_segment ;
				delete from podcast_episode ;
				delete from managed_file ;
				delete from event_publication ;
				""";
		this.destinationJdbcClient.sql(sql).update();

		var destinationPodcastId = newApiClient.getPodcastId();

		for (var podcast : podcasts)
			migrate(destinationPodcastId, podcast);

		log.debug("finished processing all the episodes!");
		this.publisher.publishEvent(new MigratedFilesDownloadedEvent());
		return UUID.randomUUID().toString();
	}

	record IncompleteFileEvent(File file, String s3Url) {

	}

	record MigratedFilesDownloadedEvent() {

	}

	@ApplicationModuleListener
	void incompleteFileEvent(IncompleteFileEvent incompleteFileEvent) {
		this.incompleteFileEvents.add(incompleteFileEvent);
	}

	@ApplicationModuleListener
	void finished(MigratedFilesDownloadedEvent m) throws Exception {
		var lines = this.incompleteFileEvents.stream()
			.map(ife -> ife.s3Url() + ":" + ife.file())
			.collect(Collectors.joining(System.lineSeparator()));
		try (var fw = new FileWriter(SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/missing.txt"))) {
			FileCopyUtils.copy(lines, fw);
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
		var photo = matchMediaByTag(media, "photo");
		var intro = matchMediaByTag(media, "introduction");
		var interview = matchMediaByTag(media, "interview");
		var produced = matchMediaByTag(media, "produced");
		var hasValidFiles = produced != null
				|| (photo != null && intro != null && interview != null && StringUtils.hasText(photo.href())
						&& StringUtils.hasText(intro.href()) && StringUtils.hasText(interview.href()));
		if (!hasValidFiles)
			return;
		var completableFutures = new HashSet<CompletableFuture<?>>();
		for (var m : new OldApiClient.Media[] { produced, interview, intro, photo }) {
			if (m != null) {
				var completableFuture = CompletableFuture.supplyAsync(() -> this.mediaDownloader.apply(m),
						this.executor);
				completableFutures.add(completableFuture);
			}
		}
		CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[] {})).get();
		System.out.println("have all the files locally...");
		// todo graphql client to get segments and update a segment and create a segment
		var episode = this.newApiClient.podcastEpisodeById(podcastEpisodeDraftId);

		// first write the photo to the managedFile
		var photoFile = download(photo.href());
		// todo write this File to the new episodes `photo` ManagedFile
		this.newApiClient.writeManagedFile(episode.graphic().id(),
				new FileSystemResource(Objects.requireNonNull(photoFile)));

		log.debug("wrote managed file for photo for episode [" + episode.graphic().id() + "]");
		for (var segment : episode.segments()) {
			var audioManagedFile = segment.audio();
			var producedAudioManagedFile = segment.producedAudio();

		}
		log.debug("got the episode " + episode);

	}

	private File download(String s3Url) {
		try {
			var parts = s3Url.split("/");
			var bucket = parts[2];
			var key = parts[3] + "/" + parts[4];
			var localFile = new File(this.oldManagedFileSystem, bucket + "/" + key);
			if (localFile.length() > 0)
				return localFile;

			var read = storage.read(bucket, key);

			if (null == read)
				return null;

			var cl = read.contentLength();

			if (!localFile.exists() || (cl != localFile.length())) {
				System.out.println("contentLength: " + cl + ":" + localFile.length());
				System.out.println("downloading [" + localFile.getAbsolutePath() + "] on ["
						+ Thread.currentThread().getName() + "].");
				var directory = localFile.getParentFile();
				if (!directory.exists())
					directory.mkdirs();
				Assert.state(directory.exists(), "the directory [" + directory.getAbsolutePath() + "] does not exist");

				read = storage.read(bucket, key); // don't want to work on the same
				try (var in = new BufferedInputStream(read.getInputStream());
						var out = new BufferedOutputStream(new FileOutputStream(localFile))) {
					FileCopyUtils.copy(in, out);
				}
				Assert.state(localFile.exists(), "the local file must exist now, but doesn't");
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
