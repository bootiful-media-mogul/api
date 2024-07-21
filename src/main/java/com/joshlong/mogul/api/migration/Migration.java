package com.joshlong.mogul.api.migration;

import com.joshlong.mogul.api.managedfiles.Storage;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.graphql.client.GraphQlClient;
import org.springframework.graphql.client.HttpSyncGraphQlClient;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.http.HttpHeaders;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.util.SystemPropertyUtils;
import org.springframework.web.client.RestClient;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * <a href=
 * "https://github.com/spring-tips/spring-graphql-redux/blob/main/live/5-clients/src/main/java/com/example/clients/ClientsApplication.java">
 * see this page for examples on using the Spring GraphQL client</a>
 */
@Controller
class MigrationController {

	private final Migration migration;

	MigrationController(Migration migration) {
		this.migration = migration;
	}

	@MutationMapping
	CompletableFuture<Void> migrate() throws Exception {
		var authentication = (JwtAuthenticationToken) SecurityContextHolder //
			.getContextHolderStrategy() //
			.getContext() //
			.getAuthentication();
		var token = authentication.getToken().getTokenValue();
		return CompletableFuture.runAsync(() -> {
			try {
				this.migration.start(token);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

}

@Component
class Migration {

	private static final String API_ROOT = "http://127.0.0.1:8080";

	private final JdbcClient sourceJdbcClient, destinationJdbcClient;

	private final Storage storage;

	private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	private final File oldManagedFileSystem = new File(SystemPropertyUtils.resolvePlaceholders("${HOME}/Desktop/old/"));

	Migration(DataSource destination, @Value("${legacy.db.username:mogul}") String username, //
			@Value("${legacy.db.password:mogul}") String password, //
			@Value("${legacy.db.host:localhost}") String host, //
			@Value("${legacy.db.schema:legacy}") String schema, Storage storage //
	) {
		this.storage = storage;
		this.sourceJdbcClient = JdbcClient.create(dataSource(username, password, host, schema));
		this.destinationJdbcClient = JdbcClient.create(destination);
		Assert.notNull(this.storage, "the storage should not be null");
		Assert.state(this.oldManagedFileSystem.exists() || this.oldManagedFileSystem.mkdirs(),
				"the directory [" + this.oldManagedFileSystem.getAbsolutePath() + "] does not exist");
	}

	private HttpSyncGraphQlClient client(String bearerToken) {
		var wc = RestClient.builder()
			.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
			.baseUrl(API_ROOT + "/graphql")
			.build();
		return HttpSyncGraphQlClient.create(wc);
	}

	private static DataSource dataSource(String username, String password, String host, String dbSchema) {
		var jdbc = new JdbcConnectionDetails() {
			@Override
			public String getUsername() {
				return username;
			}

			@Override
			public String getPassword() {
				return password;
			}

			@Override
			public String getJdbcUrl() {
				return "jdbc:postgresql://" + host + "/" + dbSchema;
			}
		};
		return dataSource(jdbc);
	}

	private static HikariDataSource dataSource(JdbcConnectionDetails connectionDetails) {
		return DataSourceBuilder //
			.create(Migration.class.getClassLoader())
			.type(HikariDataSource.class)
			.driverClassName(connectionDetails.getDriverClassName())
			.url(connectionDetails.getJdbcUrl())
			.username(connectionDetails.getUsername())
			.password(connectionDetails.getPassword())
			.build();
	}

	record Podcast(Integer id, Date date, String description, String notes, String podbeanDraftCreated,
			String podbeanDraftPublished, String podbeanMediaUri, String podbeanPhotoUri, String s3AudioFileName,
			String s3AudioUri, String s3PhotoFileName, String s3PhotoUri, String title, String transcript, String uid,
			Collection<Media> media) {

	}

	record Media(Integer id, String description, String extension, String fileName, String href, String type) {

	}

	record PodcastMedia(Integer podcastId, Integer mediaId) {

	}

	Collection<PodcastMedia> podcastMedia() {
		return this.sourceJdbcClient.sql("select * from podcast_media")
			.query((rs, rowNum) -> new PodcastMedia(rs.getInt("podcast_id"), rs.getInt("media_id")))
			.list();
	}

	Collection<Podcast> podcasts(Collection<Media> media) {
		var mediaMap = media.stream().collect(Collectors.toMap(Media::id, media12 -> media12));
		var podcastMedia = this.podcastMedia();
		return this.sourceJdbcClient.sql("select * from podcast").query((rs, rowNum) -> {
			var podcast = new Podcast(rs.getInt("id"), rs.getDate("date"), rs.getString("description"),
					rs.getString("notes"), rs.getString("podbean_draft_created"),
					rs.getString("podbean_draft_published"), rs.getString("podbean_media_uri"),
					rs.getString("podbean_photo_uri"), rs.getString("s3_audio_file_name"), rs.getString("s3_audio_uri"),
					rs.getString("s3_photo_file_name"), rs.getString("s3_photo_uri"), rs.getString("title"),
					rs.getString("transcript"), rs.getString("uid"), new HashSet<>());

			podcastMedia.stream()
				// find the PodcastMedia tables that match this podcast
				.filter(pm -> pm.podcastId().equals(podcast.id()))
				// for each PM, find the media and it to the podcast's media collection
				.forEach(pm -> {
					var mediaId = pm.mediaId();
					if (mediaMap.containsKey(mediaId))
						podcast.media().add(mediaMap.get(mediaId));
				});

			return podcast;
		})//
			.list();
	}

	Collection<Media> media() {
		return this.sourceJdbcClient.sql("select * from media")
			.query((rs, rowNum) -> new Media(rs.getInt("id"), rs.getString("description"), rs.getString("extension"),
					rs.getString("file_name"), rs.getString("href"), rs.getString("type")))
			.list();
	}

	String start(String bearer) throws Exception {
		var media = this.media();
		var podcasts = this.podcasts(media);
		var gql = this.client(bearer);
		var sql = """
				delete from managed_file_deletion_request ;
				delete from publication ;
				delete from podcast_episode_segment ;
				delete from podcast_episode ;
				delete from managed_file ;
				delete from event_publication ;
				""";
		this.destinationJdbcClient.sql(sql).update();

		var httpRequestDocument = """
				query {
				 podcasts { id   }
				}
				""";
		var destinationPodcastId = Long.parseLong((String) gql.document(httpRequestDocument)
			.retrieveSync("podcasts")
			.toEntityList(new ParameterizedTypeReference<Map<String, Object>>() {
			})
			.getFirst()
			.get("id"));

		var migration = new HashSet<CompletableFuture<Void>>();

		for (var podcast : podcasts)
			migration.add(CompletableFuture.runAsync(() -> {
				try {
					migrate(gql, destinationPodcastId, podcast);
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			}));

		CompletableFuture.allOf(migration.toArray(new CompletableFuture[0])).get();

		System.out.println("finished processing all the episodes!");
		return UUID.randomUUID().toString();
	}

	private File download(String s3Url) throws Exception {
		var parts = s3Url.split("/");
		var bucket = parts[2];
		var key = parts[3] + "/" + parts[4];
		var localFile = new File(this.oldManagedFileSystem, bucket + "/" + key);
		var read = this.storage.read(bucket, key);

		if (null == read)
			return null;

		var cl = read.contentLength();

		if (!localFile.exists() || (cl != localFile.length())) {
			System.out.println("contentLength: " + cl + ":" + localFile.length());
			System.out.println(
					"downloading [" + localFile.getAbsolutePath() + "] on [" + Thread.currentThread().getName() + "].");
			var directory = localFile.getParentFile();
			if (!directory.exists())
				directory.mkdirs();
			Assert.state(directory.exists(), "the directory [" + directory.getAbsolutePath() + "] does not exist");

			read = this.storage.read(bucket, key); // don't want to work on the same
													// InputStream twice
			try (var in = new BufferedInputStream(read.getInputStream());
					var out = new BufferedOutputStream(new FileOutputStream(localFile))) {
				FileCopyUtils.copy(in, out);
			}
			Assert.state(localFile.exists(), "the local file must exist now, but doesn't");
		}
		Assert.state(localFile.exists() && localFile.isFile(), "the local file must " + "be a file, not a directory");
		return localFile;

	}

	private void migrate(GraphQlClient client, Long newPodcastId, Podcast oldEpisode) throws Exception {
		var gql = """
				mutation CreatePodcastEpisodeDraft ($podcast: ID, $title: String, $description: String ){
				      createPodcastEpisodeDraft( podcastId: $podcast, title: $title, description: $description) {
				           id
				      }
				     }
				""";
		var podcastEpisodeDraftId = Long.parseLong((String) client.document(gql)
			.variable("podcast", newPodcastId)
			.variable("title", oldEpisode.title())
			.variable("description", oldEpisode.description())
			.executeSync()
			.field("createPodcastEpisodeDraft")
			.toEntity(new ParameterizedTypeReference<Map<String, Object>>() {
			})
			.get("id"));

		this.destinationJdbcClient.sql("update podcast_episode set created = ? where id = ?")
			.params(oldEpisode.date(), podcastEpisodeDraftId)
			.update();

		// todo download the audio from the source and then attach it to a segment
		var media = oldEpisode.media();

		class MediaPredicate implements Predicate<Media> {

			final String tag;

			MediaPredicate(String tag) {
				this.tag = tag;
			}

			@Override
			public boolean test(Media media) {
				return media != null && StringUtils.hasText(media.href()) && media.type().equals(this.tag);
			}

		}

		var photo = media.stream().filter(new MediaPredicate("photo")).findAny().orElse(null);
		var intro = media.stream().filter(new MediaPredicate("introduction")).findAny().orElse(null);
		var interview = media.stream().filter(new MediaPredicate("interview")).findAny().orElse(null);
		var produced = media.stream().filter(new MediaPredicate("produced")).findAny().orElse(null);
		var hasValidFiles = produced != null
				|| (photo != null && intro != null && interview != null && StringUtils.hasText(photo.href())
						&& StringUtils.hasText(intro.href()) && StringUtils.hasText(interview.href()));
		// Assert.state(hasValidFiles, "there must be valid files for [" + oldEpisode.id()
		// + "]");
		if (!hasValidFiles)
			return;

		class MediaDownloadingSupplier implements Supplier<File> {

			private final Media media;

			MediaDownloadingSupplier(Media media) {
				this.media = media;
			}

			@Override
			public File get() {
				try {
					if (this.media != null && StringUtils.hasText(this.media.href())) {
						return download(this.media.href());
					}
					return null;
				} //
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

		}

		var cfs = new HashSet<CompletableFuture<?>>();
		for (var m : new Media[] { produced, interview, intro, photo }) {
			if (m != null) {
				cfs.add(CompletableFuture.supplyAsync(new MediaDownloadingSupplier(m)));
			}
		}
		var awaitDownloads = CompletableFuture.allOf(cfs.toArray(new CompletableFuture[] {}));
		awaitDownloads.get();
		// System.out.println("everything is downloaded for podcast " +
		// podcastEpisodeDraftId);
		if (produced != null) {
			// download(produced.href());

		} //
		else {
			if (photo != null) {

			} //
			else {
				System.out.println("photo is null for [" + podcastEpisodeDraftId + "]");
			}

			if (intro != null) {

			} //
			else {
				System.out.println("intro is null for [" + podcastEpisodeDraftId + "]");
			}

			if (interview != null) {

			} //
			else {
				System.out.println("interview is null for [" + podcastEpisodeDraftId + "]");
			}
			// var interview = media.stream().filter( m ->
			// m.type().equals("interview")).findFirst().get();

		}

	}

}
