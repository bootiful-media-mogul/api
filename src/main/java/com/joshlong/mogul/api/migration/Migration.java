package com.joshlong.mogul.api.migration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.mogul.api.PublisherPlugin;
import com.joshlong.mogul.api.Settings;
import com.joshlong.mogul.api.managedfiles.CommonMediaTypes;
import com.joshlong.mogul.api.managedfiles.ManagedFile;
import com.joshlong.mogul.api.managedfiles.ManagedFileService;
import com.joshlong.mogul.api.managedfiles.Storage;
import com.joshlong.mogul.api.mogul.MogulService;
import com.joshlong.mogul.api.podcasts.publication.PodbeanPodcastEpisodePublisherPlugin;
import com.joshlong.mogul.api.publications.DefaultPublicationService;
import com.joshlong.mogul.api.publications.PublicationService;
import com.joshlong.mogul.api.publications.SettingsLookupClient;
import com.joshlong.mogul.api.utils.JsonUtils;
import com.joshlong.podbean.Episode;
import com.joshlong.podbean.PodbeanClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.security.crypto.encrypt.TextEncryptor;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.SystemPropertyUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.joshlong.mogul.api.podcasts.publication.PodbeanPodcastEpisodePublisherPlugin.*;

class Migration {

	private final PublicationService defaultPublicationService;

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

	private final TextEncryptor textEncryptor;

	private final OldApiClient oldApiClient;

	/**
	 * this gets called from online with access to a token
	 */
	public void migrateAllPodcasts(boolean reset, String token) {
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
			var podbeanEpisodes = podbeanClient.getAllEpisodes();
			var podcastsToPodbeanEpisodes = new HashMap<OldApiClient.Podcast, Episode>();
			for (var podcast : podcasts) {
				var title = podcast.title();
				var episode = podbeanEpisodes.stream().filter(e -> e.getTitle().equalsIgnoreCase(title)).findAny();
				episode.ifPresent(value -> podcastsToPodbeanEpisodes.put(podcast, value));
				log.info("migrated legacy podcast #" + podcast.id());

			}

			record MogulEpisode(Long episodeId, String title) {
			}

			var mogulEpisodes = this.destinationJdbcClient.sql("select id ,title from mogul.public.podcast_episode")
				.query((rs, rowNum) -> new MogulEpisode(rs.getLong("id"), rs.getString("title")))
				.list()
				.stream()
				.collect(Collectors.toMap(MogulEpisode::title, e -> e));

			podcastsToPodbeanEpisodes.forEach((podcast, episode) -> {
				var mogulEpisodeId = mogulEpisodes.get(podcast.title()).episodeId();
				this.publish(episode.getPodcastId(), episode.getId(), mogulEpisodeId);
			});

			System.out.println("size:" + podcastsToPodbeanEpisodes.size());

			// try (var fw = new BufferedWriter(new FileWriter(this.managedFileLog))) {
			// fw.write(JsonUtils.write(this.managedFileWrites));
			// }
			//
			// this.publisher.publishEvent(new ManagedFilesReadyForWriteEvent());

		} //
		catch (Throwable throwable) {
			log.error("no idea", throwable);
			throw new RuntimeException(throwable);
		}
	}

	private final NewApiClient newApiClient;

	private final PodbeanClient podbeanClient;

	private final SettingsLookupClient lookup;

	Migration(ManagedFileService managedFileService, Storage storage, JdbcClient destinationJdbcClient,
			ApplicationEventPublisher publisher, OldApiClient oldApiClient, NewApiClient newApiClient,
			MogulService mogulService, Settings settings, TextEncryptor textEncryptor,
			Map<String, PublisherPlugin<?>> pluginMap, ObjectMapper objectMapper, PodbeanClient podbeanClient) {
		this.managedFileService = managedFileService;
		this.storage = storage;
		this.publisher = publisher;
		this.oldApiClient = oldApiClient;
		this.newApiClient = newApiClient;
		this.destinationJdbcClient = destinationJdbcClient;
		this.podbeanClient = podbeanClient;
		this.lookup = new SettingsLookupClient(settings);
		this.textEncryptor = textEncryptor;
		this.defaultPublicationService = new DefaultPublicationService(this.destinationJdbcClient, mogulService,
				textEncryptor, this.lookup, pluginMap, objectMapper);
		Assert.state(this.existingS3Files.exists() || this.existingS3Files.mkdirs(),
				"the directory [" + this.existingS3Files.getAbsolutePath() + "] does not exist");
	}

	private ManagedFile writeManagedFileToS3(Long managedFileId, String originalFileName, Resource resource) {
		var managedFile = this.managedFileService.getManagedFile(managedFileId);
		if (managedFile.written())
			return managedFile;
		log.info("writing managed file #" + managedFileId + " [" + originalFileName + "]");
		var mediaType = CommonMediaTypes.guess(resource);
		this.managedFileService.write(managedFileId, originalFileName, mediaType, resource);
		var updated = this.managedFileService.getManagedFile(managedFileId);
		Assert.state(updated.written(), "the managedFile is written!");
		log.info("wrote managed file #" + managedFileId + " [" + originalFileName + "]");
		return updated;
	}

	@Async
	@EventListener(ManagedFilesReadyForWriteEvent.class)
	void afterMigration() throws Exception {
		// writePendingManagedFilesListener();
	}

	@EventListener({ ApplicationReadyEvent.class })
	void afterLoad() throws Exception {
		// writePendingManagedFilesListener();
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

	private void publish(String podbeanPodcastId, String podbeanEpisodeId, Long publicationKey // mogul
																								// episode
																								// id
	/* com.joshlong.mogul.api.podcasts.Episode mogulEpisode */) {

		var mogulId = 16386L;
		var kh = new GeneratedKeyHolder();
		var entityClass = Episode.class.getName();

		var context = new ConcurrentHashMap<String, String>();
		context.put(CONTEXT_PODBEAN_PODCAST_ID, podbeanPodcastId);
		context.put(CONTEXT_PODBEAN_EPISODE_ID, podbeanEpisodeId);
		context.put(CONTEXT_PODBEAN_EPISODE_PUBLISH_DATE_IN_MILLISECONDS, Long.toString(new Date().getTime()));

		var contextJson = this.textEncryptor.encrypt(JsonUtils.write(context));
		var publicationData = this.textEncryptor.encrypt(JsonUtils.write(publicationKey));
		var configuration = this.lookup.apply(new DefaultPublicationService.SettingsLookup(mogulId, PLUGIN_NAME));
		context.putAll(configuration);

		this.destinationJdbcClient.sql(
				"insert into publication(mogul_id, plugin, created, published, context, payload , payload_class) VALUES (?,?,?,?,?,?,?)")
			.params(mogulId, PodbeanPodcastEpisodePublisherPlugin.PLUGIN_NAME, new Date(), null, contextJson,
					publicationData, entityClass)
			.update(kh);

		System.out
			.println("inserting publication for " + podbeanPodcastId + ":" + podbeanEpisodeId + ":" + publicationKey);
		// defaultPublicationService.publish(16386, Map.of(), )
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
	// system
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
				log.info("downloaded " + s3Url + " to " + localFile.getAbsolutePath() + '.');

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