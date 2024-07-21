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
import org.springframework.web.client.RestClient;

import javax.sql.DataSource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * <a href="https://github.com/spring-tips/spring-graphql-redux/blob/main/live/5-clients/src/main/java/com/example/clients/ClientsApplication.java">
 * see this page for examples on using the Spring GraphQL client</a>
 */
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

	private final JdbcClient sourceJdbcClient, destinationJdbcClient;

	private HttpSyncGraphQlClient client(String bearerToken) {
		var wc = RestClient.builder()
			.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
			.baseUrl(API_ROOT + "/graphql")
			.build();
		return HttpSyncGraphQlClient.create(wc);
	}

	Migration(
			DataSource destination,
			@Value("${legacy.db.username:mogul}") String username, //
			@Value("${legacy.db.password:mogul}") String password, //
			@Value("${legacy.db.host:localhost}") String host, //
			@Value("${legacy.db.schema:legacy}") String schema //
	) {
		this.sourceJdbcClient = JdbcClient.create(dataSource(username, password, host, schema));
		this.destinationJdbcClient = JdbcClient.create(destination);
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

		podcasts.forEach(podcast -> {
			try {
				migrate(gql, destinationPodcastId, podcast);
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});

		return UUID.randomUUID().toString();

	}

	private void migrate(GraphQlClient client, Long newPodcastId, Podcast oldEpisode) throws Exception {
		var gql = """
				mutation CreatePodcastEpisodeDraft ($podcast: ID, $title: String, $description: String ){ 
				      createPodcastEpisodeDraft( podcastId: $podcast, title: $title, description: $description) { 
				           id  
				      }
				     }
				""";
		var podcastEpisodeDraftId = client
				.document(gql)
				.variable("podcast", newPodcastId)
				.variable("title", oldEpisode.title())
				.variable("description", oldEpisode.description())
				.executeSync()
				.field("createPodcastEpisodeDraft")
				.toEntity(new ParameterizedTypeReference<Map<String, Object>>() {
				});
		System.out.println("podcast draft episode ID: " + podcastEpisodeDraftId);

		// todo download the audio from the source and then attach it to a segment
		var media = oldEpisode.media();
		var photo = media.stream().filter(m -> m.type().equals("photo")).findAny().orElse(null);
		var intro = media.stream().filter(m -> m.type().equals("intro")).findAny().orElse(null);
		var interview = media.stream().filter(m -> m.type().equals("interview")).findAny().orElse(null);
//		var interview = media.stream().filter( m -> m.type().equals("interview")).findFirst().get();



	}

}
