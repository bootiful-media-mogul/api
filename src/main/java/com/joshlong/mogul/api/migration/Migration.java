package com.joshlong.mogul.api.migration;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.stream.Collectors;

@Profile("migration")
@Configuration
class Migration {

    private final DataSource source;

    private final DataSource destination;

    private final JdbcClient sourceJdbcClient, destinationJdbcClient;

    Migration(@Value("${legacy.db.username:mogul}") String username, @Value("${legacy.db.password:mogul}") String password, @Value("${legacy.db.host:localhost}") String host,
              @Value("${legacy.db.schema:legacy}") String schema, DataSource destination) {
//        this.client =  GraphQlClient.builder(new HttpGraphQlTransport ()).build();

        this.source = dataSource(username, password, host, schema);
        this.destination = destination;
        Assert.notNull(this.source, "the source can not be null");
        Assert.notNull(this.destination, "the source can not be null");

        this.destinationJdbcClient = JdbcClient.create(this.destination);
        this.sourceJdbcClient = JdbcClient.create(this.source);
    }

    private static DataSource dataSource(
            String username, String password,
            String host, String dbSchema) {
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
        return DataSourceBuilder
                .create(Migration.class.getClassLoader())
                .type(HikariDataSource.class)
                .driverClassName(connectionDetails.getDriverClassName())
                .url(connectionDetails.getJdbcUrl())
                .username(connectionDetails.getUsername())
                .password(connectionDetails.getPassword())
                .build();
    }

    record Podcast(
            Integer id,
            Date date,
            String description,
            String notes,
            String podbeanDraftCreated,
            String podbeanDraftPublished,
            String podbeanMediaUri,
            String podbeanPhotoUri,
            String s3AudioFileName,
            String s3AudioUri,
            String s3PhotoFileName,
            String s3PhotoUri,
            String title,
            String transcript,
            String uid, Collection<Media> media
    ) {
    }

    record Media(Integer id, String description, String extension,
                 String fileName, String href, String type) {
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
        return this.sourceJdbcClient
                .sql("select * from podcast")
                .query((rs, rowNum) -> {
                    var podcast = new Podcast(
                            rs.getInt("id"),
                            rs.getDate("date"),
                            rs.getString("description"),
                            rs.getString("notes"),
                            rs.getString("podbean_draft_created"),
                            rs.getString("podbean_draft_published"),
                            rs.getString("podbean_media_uri"),
                            rs.getString("podbean_photo_uri"),
                            rs.getString("s3_audio_file_name"),
                            rs.getString("s3_audio_uri"),
                            rs.getString("s3_photo_file_name"),
                            rs.getString("s3_photo_uri"),
                            rs.getString("title"),
                            rs.getString("transcript"),
                            rs.getString("uid"),
                            new HashSet<>());

                    podcastMedia
                            .stream()
                            // find the PodcastMedia tables that match this podcast
                            .filter(pm -> pm.podcastId().equals(podcast.id()))
                            // for each PM, find the media and it to the podcast's media collection
                            .forEach(pm -> {
                                var mediaId = pm.mediaId();
                                if (mediaMap.containsKey(mediaId))
                                    podcast.media().add(mediaMap.get(mediaId));
                            });

                    return podcast;
                })
                .list();
    }

    Collection<Media> media() {
        return this.sourceJdbcClient
                .sql("select * from media").query((rs, rowNum) -> new Media(rs.getInt("id"),
                        rs.getString("description"),
                        rs.getString("extension"), rs.getString("file_name"),
                        rs.getString("href"), rs.getString("type")))
                .list();
    }

    @Bean
    ApplicationRunner migrationApplicationRunner() {
        return args -> {
            var media = this.media();
            var podcasts = this.podcasts(media);
            podcasts.forEach(this::migrate);
        };
    }


    private void migrate(Podcast podcast) {
        System.out.println(podcast.toString());
    }
}

/*

@Service
class MigrationClient {

    

    */
/* HttpGraphQlClient http){
		return args -> {

			var httpRequestDocument = """

					query {
					 customerById(id:1){
					  id, name
					 }
					}

					""" ;

			http.document(httpRequestDocument).retrieve("customerById").toEntity(Customer.class)
					.subscribe(System.out::println);


			var rsocketRequestDocument = """

					subscription {
					 greetings { greeting }
					}

					""" ;
			rsocket.document(rsocketRequestDocument)
					.retrieveSubscription("greetings")
					.toEntity(Greeting.class)
					.subscribe(System.out::println);
		};
	}*//*


    @Autowired
    private GraphQLTestTemplate graphQLTestTemplate;

    // Queries
    public String getNotifications() {
        String query = "{ notifications { mogulId category key when context modal visible } }";
        GraphQLResponse response = graphQLTestTemplate.postForResource(query);
        return response.getRawResponse().toString();
    }

    public String getSettings() {
        String query = "{ settings { valid category settings { name value valid } } }";
        GraphQLResponse response = graphQLTestTemplate.postForResource(query);
        return response.getRawResponse().toString();
    }

    public String getMogul() {
        String query = "{ me { name } }";
        GraphQLResponse response = graphQLTestTemplate.postForResource(query);
        return response.getRawResponse().toString();
    }

    public String aiChat(String prompt) {
        String query = String.format("{ aiChat(prompt: \"%s\") }", prompt);
        GraphQLResponse response = graphQLTestTemplate.postForResource(query);
        return response.getRawResponse().toString();
    }

    public String getPodcastById(String id) {
        String query = String.format("{ podcastById(id: \"%s\") { title episodes { id title } id } }", id);
        GraphQLResponse response = graphQLTestTemplate.postForResource(query);
        return response.getRawResponse().toString();
    }

    public String getPodcasts() {
        String query = "{ podcasts { title episodes { id title } id } }";
        GraphQLResponse response = graphQLTestTemplate.postForResource(query);
        return response.getRawResponse().toString();
    }

    public String getPodcastEpisodeById(String id) {
        String query = String.format("{ podcastEpisodeById(id: \"%s\") { id title description valid graphic { id } complete created segments { id name } } }", id);
        GraphQLResponse response = graphQLTestTemplate.postForResource(query);
        return response.getRawResponse().toString();
    }

    public String getPodcastEpisodesByPodcast(String podcastId) {
        String query = String.format("{ podcastEpisodesByPodcast(podcastId: \"%s\") { id title description valid graphic { id } complete created segments { id name } } }", podcastId);
        GraphQLResponse response = graphQLTestTemplate.postForResource(query);
        return response.getRawResponse().toString();
    }

    public String getManagedFileById(String id) {
        String query = String.format("{ managedFileById(id: \"%s\") { id bucket folder filename contentType size written } }", id);
        GraphQLResponse response = graphQLTestTemplate.postForResource(query);
        return response.getRawResponse().toString();
    }

    // Mutations
    public String publishPodcastEpisode(String episodeId, String pluginName) {
        String mutation = String.format("mutation { publishPodcastEpisode(episodeId: \"%s\", pluginName: \"%s\") }", episodeId, pluginName);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String updatePodcastEpisode(String episodeId, String title, String description) {
        String mutation = String.format("mutation { updatePodcastEpisode(episodeId: \"%s\", title: \"%s\", description: \"%s\") { id title description valid graphic { id } complete created segments { id name } } }", episodeId, title, description);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String createPodcast(String title) {
        String mutation = String.format("mutation { createPodcast(title: \"%s\") { title episodes { id title } id } }", title);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String createPodcastEpisodeDraft(String podcastId, String title, String description) {
        String mutation = String.format("mutation { createPodcastEpisodeDraft(podcastId: \"%s\", title: \"%s\", description: \"%s\") { id title description valid graphic { id } complete created segments { id name } } }", podcastId, title, description);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String deletePodcast(String id) {
        String mutation = String.format("mutation { deletePodcast(id: \"%s\") }", id);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String deletePodcastEpisode(String id) {
        String mutation = String.format("mutation { deletePodcastEpisode(id: \"%s\") }", id);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String deletePodcastEpisodeSegment(String id) {
        String mutation = String.format("mutation { deletePodcastEpisodeSegment(id: \"%s\") }", id);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String addPodcastEpisodeSegment(String episodeId) {
        String mutation = String.format("mutation { addPodcastEpisodeSegment(episodeId: \"%s\") }", episodeId);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String movePodcastEpisodeSegmentUp(String episodeId, int episodeSegmentId) {
        String mutation = String.format("mutation { movePodcastEpisodeSegmentUp(episodeId: \"%s\", episodeSegmentId: %d) }", episodeId, episodeSegmentId);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String movePodcastEpisodeSegmentDown(String episodeId, int episodeSegmentId) {
        String mutation = String.format("mutation { movePodcastEpisodeSegmentDown(episodeId: \"%s\", episodeSegmentId: %d) }", episodeId, episodeSegmentId);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }

    public String updateSetting(String category, String name, String value) {
        String mutation = String.format("mutation { updateSetting(category: \"%s\", name: \"%s\", value: \"%s\") }", category, name, value);
        GraphQLResponse response = graphQLTestTemplate.postForResource(mutation);
        return response.getRawResponse().toString();
    }
}
*/
