package com.joshlong.mogul.api.migration;

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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.RestClient;

import javax.sql.DataSource;
import java.security.Principal;
import java.util.*;
import java.util.stream.Collectors;

@Controller
class MigrationController {

	private final Migration migration;

	MigrationController(Migration migration) {
		this.migration = migration;
	}

	@MutationMapping
	String migrate() throws Exception {
		var authentication = (JwtAuthenticationToken) SecurityContextHolder //
			.getContextHolderStrategy() //
			.getContext() //
			.getAuthentication();
		var token = authentication.getToken().getTokenValue();
		return this.migration.start(token);
	}

}

@Component
class Migration {

	private static final String API_ROOT = "http://127.0.0.1:8080";

	private final DataSource source;

	private final JdbcClient sourceJdbcClient;

	private HttpSyncGraphQlClient client(String bearerToken) {
		var wc = RestClient.builder()
			.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
			.baseUrl(API_ROOT + "/graphql")
			.build();
		return HttpSyncGraphQlClient.create(wc);
	}

	Migration(@Value("${legacy.db.username:mogul}") String username,
			@Value("${legacy.db.password:mogul}") String password, @Value("${legacy.db.host:localhost}") String host,
			@Value("${legacy.db.schema:legacy}") String schema) {
		this.source = dataSource(username, password, host, schema);
		Assert.notNull(this.source, "the source can not be null");
		this.sourceJdbcClient = JdbcClient.create(this.source);
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

	String start(String bearer) {
		var media = this.media();
		var podcasts = this.podcasts(media);
		var gql = this.client(bearer);

		var httpRequestDocument = """
				query {
				 podcasts { id   }
				}
				""";
		System.out.println(gql.document(httpRequestDocument)
			.retrieveSync("podcasts")
			.toEntityList(new ParameterizedTypeReference<Map<String, Object>>() {
			}));

		podcasts.forEach(podcast -> {
			try {
				migrate(gql, podcast);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		return UUID.randomUUID().toString();

	}

	private void migrate(GraphQlClient client, Podcast podcast) throws Exception {
		// System.out.println(podcast.toString());
	}

}
